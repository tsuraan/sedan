"""This is the core of our couch wrapper.  The CouchBatch class defined here
is what end-users can use to efficiently query and update couchdb.
"""
from couchdbkit.exceptions import ResourceConflict
from couchdbkit.exceptions import ResourceNotFound
from couchdbkit.exceptions import BulkSaveError
from functools             import partial
from pprint                import pprint as pp
import copy

from .exceptions import BatchJobNeedsDocument
from .promise    import Promise
from .result     import DbFailure
from .result     import DbValue
from .jobs       import ReadJob
from .jobs       import CreateJob
from .jobs       import ReplaceJob
from .jobs       import UpdateJob
from .jobs       import DeleteJob

class CouchBatch(object):
  """An efficient job batcher for CouchDB.  All the database-access functions
  of this class (.get, .create, .overwrite, .update, and .delete) return a
  promise object without accessing the database.  When that promise object's
  value() method is called, all the accumulated work that the batch has been
  given will ideally be sent to the database in a single operation.  Sometimes
  reads and retries will be necessary, but this still tends to be more
  efficient than doing a ton of reads and writes in the sane ordering that a
  programmer wants to use.

  Since we batch operations for efficiency, actions on the same key may cause
  conflicts.  Rather than coming up with creative and disturbing ways for
  operations on a single key to interact without touching the database, I've
  implemented the more sane optimizations and defined errors for the rest of
  the cases.  The top of this table is the current activity scheduled for the
  key, and the left side is the activity that the user is trying to schedule
  on top of the existing activity.  When these failures occur, it is the
  actual method that raises the exception, rather than a returned promise.
  This should make debugging of complicated batch operations a bit more sane.

  The general reasoning behind these behaviours is the question of whether the
  user could be relying on the failure of the first action as a signal.  If
  the first action can fail, and the second action would mask that failure,
  then the second action is forbidden.

             Current Action
  New Action
             | create        | overwrite     | update        | delete
  -----------+---------------+---------------+---------------+----------------
  create     | Create-       | Overwrite-    | Update-       | DeleteScheduled
             | Scheduled is  | Scheduled is  | Scheduled is  | is immediately
             | immediately   | immediately   | immediately   | raised
             | raised        | raised        | raised        |
             |               |               |               |
  overwrite  | Create-       | New activity  | Update-       | DeleteScheduled
             | Scheduled is  | overrides     | Scheduled is  | is immediately
             | immediately   | existing; both| immediately   | raised
             | raised        | promises tied | raised        |
             |               | to new one    |               |
             |               |               |               |
  update     | Update is     | Update is     | Both updates  | DeleteScheduled
             | applied to new| applied to    | joined into   | is immediately
             | document; both| overwrite doc;| composite;    | raised
             | promises are  | both promises | both promises |
             | dependent on  | dependent on  | tied to update|
             | success of    | update        | success       |
             | creation      | success       |               |
             |               |               |               |
  delete     | Create-       | Scheduled task| Update-       | DeleteScheduled
             | Scheduled is  | becomes delete| Scheduled is  | is immediately
             | immediately   | both promises | immediately   | raised
             | raised        | tied to delete| raised        |
             |               | success       |               |
  """
  def __init__(self, couchkit):
    self.__ck = couchkit
    self.clear_cache()
    self._reset()

  def clear_cache(self):
    self.__docCache = {}

  def forget(self, key):
    """Remove a given document from the cache"""
    try:
      del self.__docCache[key]
    except KeyError:
      pass

  def _reset(self):
    """Setup the operations"""
    self._writes   = {}
    self._reads    = {}
    self._stats    = { 'read' : 0, 'write' : 0, 'fromcache' : 0 }

  def do_reads(self):
    """Fulfill all of the promises outstanding on ".get" requests."""
    keys    = list(self._reads)
    results = self.__ck.all_docs(keys=keys, include_docs=True)
    self._stats['read'] += 1

    for row in results:
      key = row['key']
      if 'doc' in row:
        _fulfill(self._reads, key, DbValue(row))
        self.__docCache[key] = row
      elif row.get('error') == 'not_found':
        _fulfill(self._reads, key, DbFailure(ResourceNotFound(row)))
      else:
        raise RuntimeError("Unknown couch error type: %s" % row)

  def get(self, *keys, **kwargs):
    """Get the desired documents from couch.  This will return a dictionary of
    key -> Promise.

    Notice that the promises' values will be the actual couch response
    dictionaries, so the typical keys are 'rev', 'doc', etc.  The data you're
    looking for is probably under the key of 'doc', unless you want the doc's
    revision or something else meta about the document.

    @param keys   The keys for the documents we want to retreive
    @param cached A boolean keyword argument indicating whether we are allowed
                  to return cached values.  Defaults to True.
    """
    keys   = set(keys)
    result = {}

    if kwargs.get('cached', True):
      for key in keys:
        try:
          cached = self.__docCache[key]
          promise = Promise(lambda: None)
          promise._fulfill(DbValue(cached))
          result[key] = promise
          self._stats['fromcache'] += 1
        except KeyError:
          pass

    notcached = keys - set(result)
    for key in notcached:
      result[key] = _set_job(self._reads, key, self.do_reads,
          partial(ReadJob, key))

    return result

#  def view(self, vname, **kwargs):
#    """Run a view.  It can be useful to run a view through the couch batch
#    rather than directly through the couchkit object because when include_docs
#    is given, the docs get added to the batch's cache.
#    @param vname  The name of the view to query
#    @param kwargs CouchDB view arguments
#    @return The result of the view
#    """
#    rows = self.__ck.view(vname, **kwargs)
#    self._stats['read'] += 1
#    if kwargs.get('include_docs'):
#      for row in rows:
#        doc = row['doc']
#        rev = doc['_rev']
#        did = doc['_id']
#        if did in self.__docCache:
#          continue
#        self.__docCache[did] = {
#            'doc' : doc,
#            'id'  : did,
#            'key' : did,
#            'value' : {'rev':rev},
#            }
#    return rows

  def do_writes(self, retries=5):
    """Run the current batch of write operations.  This will fulfill all the
    promises we have outstanding on create, overwrite, update, and delete
    operations.
    """
    bulk_write  = {}
    needcurrent = []

    for job in self._writes.values():
      try:
        bulk_write[job.docid] = job.doc()
      except BatchJobNeedsDocument:
        current = self.get(job.docid)[job.docid]
        needcurrent.append( (current, job) )

    for current, job in needcurrent:
      try:
        value = current.value()
        bulk_write[job.docid] = job.doc(value)
      except ResourceNotFound, e:
        job.promise._fulfill(DbFailure(e))

    try:
      self._stats['write'] += 1
      results = self.__ck.bulk_save(bulk_write.values())
    except BulkSaveError, e:
      results = e.results

    retries = []
    for result in results:
      key = result['id']
      job = self._writes[key]
      if 'error' in result:
        if job.can_retry:
          retries.append(job)
          self.forget(job.docid)
        else:
          job.promise._fulfill(DbFailure(_make_conflict(result)))
      else:
        job.promise._fulfill(DbValue(result))
        doc = bulk_write[key]
        if doc.get('_deleted') == True:
          self.forget(key)
        else:
          doc = copy.deepcopy(doc)
          doc['_rev'] = result['rev']
          self.__docCache[key] = {
              'id' : key,
              'key' : key,
              'value' : {'rev':result['rev']},
              'doc' : doc,
              }

    self._writes = dict( (job.docid, job) for job in retries )
    if self._writes:
      if retries > 0:
        self.do_writes(retries-1)
      else:
        for job in self._writes.values():
          job.promise._fulfill(DbFailure(_make_conflict(job.docid)))
        self._writes = {}

    assert self._writes == {}

  def create(self, key, document):
    """Create a new document.  The promise returned from this will have a
    value that either raises an exception or returns a dictionary with the
    keys "rev" and "id".

    Create will fail if the document already exists in couch; this means that
    if we have the key in our doc cache, or if the document is scheduled to be
    created already, the returned promise will be a failure.
    
    @param key      The key at which to store the given document
    @param document The data to store (should be a dictionary)
    """
    if (key not in self._writes) and (key not in self.__docCache):
      # We know nothing of this key, we we'll queue it up for writing
      promise = Promise(self.do_writes)
      self._writes[key] = CreateJob(key, document, promise)
      return promise

    elif key in self._writes:
      # We know that we are supposed to be doing something with this key
      # already; this might not be a failure, however
      if isinstance(self._writes[key], DeleteJob):
        # They wanted to delete before, but now they want to create, so we'll
        # just stomp what was there
        promise = _set_job(self._writes, key, self.do_writes,
            partial(ReplaceJob, key, document, None))
        return promise

    # We know that the key is either a non-deleting write or is in our cache
    # (thus known to exist).  We'll return a promise that's already set as a
    # failure.
    promise = Promise(lambda: None)
    promise._fulfill(DbFailure(_make_conflict(key)))
    return promise

  def delete(self, key):
    """Delete a document.  The promise returned here will contain a dictionary
    with the keys "rev", "id", and "ok", or it will raise a ResourceNotFound
    exception.

    If there is already a job pending for the given key, that job will be
    completed with a failure of ResourceNotFound, since it was logically
    deleted before it could be created or modified in the database.

    @param key  The key of the document to delete.
    """
    try:
      current = self._writes[key]
    except KeyError:
      pass
    else:
      current.promise._fulfill(DbFailure(ResourceNotFound))

    promise = Promise(self.do_writes)
    self._writes[key] = DeleteJob(key, promise)
    return promise


  def update(self, key, updatefn):
    """Queue up a document update function.  On commit, the document
    associated with given key will be retrieved, and the function will be
    called on the document.  If the document does not exist in the database,
    the given function will be given an empty dictionary as its argument.

    The result of the function will be the new value that will be stored (at
    least, given to the other updates for the document).  If the update
    function returns None, it will have no effect on the stored value of the
    document.

    If the key is already queued for deletion, the updatefn will be
    immediately called with an empty dicionary.  If the updatefn returns
    non-None, then the key will be removed from deletion and the returned
    document will be put in the creation queue as an __overwrite document.

    If the key is queued for creation, the updatefn will be called on the
    queued doc.  If it returns none-None, the result will replace the one
    already in the creation queue.

    @param key      The key of the document to update
    @param updatefn The function that will transform the document data
    """

def _fulfill(jobs, key, result):
  """Complete the promises outstanding for the given document key.

  @param jobs   The key -> Job dictionary for read or write jobs
  @param key    The couch key for the promise
  @param result The DbResult to record
  """
  if key in jobs:
    jobs[key].promise._fulfill(result)
    del jobs[key]

def _set_job(jobs, key, completer_fn, job_fn):
  """Assign (or re-assign) the job for the given key.  This generates a new
  Promise chained to the promise of the already-present job for the key (if
  any).  It then gives that promise to the given job_fn to generate a new Job,
  which it stores in the jobs dictionary.  The new promise is returned from
  this function.

  @param jobs         The dictionary of key -> Job
  @param key          The key we want to set the job for
  @param completer_fn The function the promise needs to call when its value is
                      requested
  @param job_fn       The function that will give us a Job
  @return             A new promise object to give to the user
  """
  try:
    prev_promise = jobs[key].promise
  except KeyError:
    prev_promise = None
  
  promise = Promise(completer_fn, prev_promise)
  jobs[key] = job_fn(promise)
  return promise

def _make_conflict(key):
  """Generate a ResourceConflict exception object for the given key"""
  conflict = ResourceConflict({
    'id'     : key,
    'error'  : 'conflict',
    'reason' : 'Document update conflict.',
    })
  return conflict

