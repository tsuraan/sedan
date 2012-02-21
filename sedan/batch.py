"""This is the core of our couch wrapper.  The CouchBatch class defined here
is what end-users can use to efficiently query and update couchdb.
"""
from couchdbkit.exceptions import ResourceNotFound
from functools import partial

from .promise import Promise
from .result  import DbFailure
from .result  import DbValue
from .jobs    import ReadJob
from .jobs    import CreateJob
from .jobs    import ReplaceJob
from .jobs    import UpdateJob
from .jobs    import DeleteJob

class CouchBatch(object):
  """This class allows the user to queue up numerous couch operations which
  will be submitted as a single save_docs call when the commit() method is
  called.  The only time this ever touches couchdb is when the commit() method
  is called; otherwise, this is completely local.
  """
  def __init__(self, couchkit):
    self.__ck = couchkit
    self.clear_cache()
    self._reset()

  def clear_cache(self):
    self.__docCache = {}

  def _reset(self):
    """Setup the operations"""
    self._writes   = {}
    self._reads    = {}
    self._stats    = { 'read' : 0, 'write' : 0, 'fromcache' : 0 }

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
    promise._fulfill(DbFailure(ResourceConflict({
      'id'     : key,
      'error'  : 'conflict',
      'reason' : 'Document update conflict.',
      })))
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
    if key in self._deletes:
      doc = updatefn({})
      if doc:
        self._deletes.remove(key)
        doc['__overwrite'] = True
        self._creates[key] = doc
      return

    if key in self._creates:
      doc = copy.deepcopy(self._creates[key])
      doc = updatefn(doc)
      if doc:
        self._creates[key] = doc
      return

    self._updates.setdefault(key, []).append(updatefn)
    self._sanity()

  def delete(self, key):
    """Queue up a document deletion.

    If the key is already in the creation or update queues, it will be removed
    from them, as the document is being deleted anyhow.

    @param key  The key of the document to delete
    """
    if key in self._creates:
      del self._creates[key]
    if key in self._updates:
      del self._updates[key]
    if key in self._deletes:
      return
    self._deletes.add(key)
    self._sanity()

  def commit(self, retries=5):
    """Do the actual database operations.  If only creates have been done,
    this can complete in a single database operation.  If that operation fails
    (usually due to conflicts), then a read will be done possibly followed by
    more write attempts.  If update or delete have been used, a query will be
    done to read in the docs associated with the keys, and then writes will be
    done as normal.
    """
    bulk_insert = []

    # Build the list of couch keys we need to fetch
    lookups = list(set(self._updates).union(self._deletes))
    for key, doc in self._creates.items():
      if '__overwrite' in doc:
        lookups.add(key)

    if lookups:
      lresult = self.get(*lookups)
      bulk_insert.extend(self._process_updates(lresult))
      bulk_insert.extend(self._process_deletes(lresult))
      bulk_insert.extend(self._process_creates(lresult))

    for key, doc in self._creates.items():
      if '__overwrite' in doc:
        # We don't want to submit the __overwrite part to couch, but we do want
        # to keep it in our real copy in case we need to retry.  create a deep
        # copy, strip __overwrite from that, and put that in the bulk_insert
        # list.
        doc = copy.deepcopy(doc)
        del doc['__overwrite']
      doc['_id'] = key
      bulk_insert.append(doc)

    try:
      # Whether or not this bulk insert works, none of our cached values for
      # the updated docs are things we really want to keep using, so we'll
      # first forget all of that
      for doc in bulk_insert:
        self.forget(doc['_id'])

      self.__ck.save_docs(bulk_insert)
      self._stats['write'] += 1
      print 'bulk save success!'
      self._reset()
      return
    except K.BulkSaveError, err:
      failures = [e['id'] for e in err.errors]
      print 'bulk save had failures in the keys:', failures
      retry_c  = {} # new creates, for retry
      retry_u  = {} # new updates, for retry
      retry_d  = {} # new deletes, for retry

      for key in failures:
        if key in self._creates:
          retry_c[key] = self._creates[key]
        elif key in self._updates:
          retry_u[key] = self._updates[key]
        elif key in self._deletes:
          retry_d[key] = self._deletes[key]
        else:
          # I think this isn't possible...
          print 'failures returned an unclaimed key!', key

      self._reset()
      if retries <= 0:
        raise BatchCommitFailure("Out of retries", retry_c, retry_u, retry_d)

      if retry_c:
        # For failures to update or delete, we can just try a few more times
        # with updated version numbers.  For creates, we need to do a bit of
        # work to determine whether the create failures are fatal or
        # ignorable.  Generally, when the user told us to create something and
        # we fail, then we have an error.  However, due to the way we do batch
        # optimizations, a delete followed by a create or updated won't be a
        # fatal error when the create fails; instead, we'll read what is in
        # the database and stomp over it.
        non_stomp = []
        for doc in retry_c.values():
          if '__overwrite' not in doc:
            non_stomp.append(doc)

        if non_stomp:
          # It looks like couch has some race conditions where a document
          # create works, but gives an error anyhow.  we want to go through
          # and make sure that we're not erroneously raising a creation error
          non_stomp_ids = [doc['_id'] for doc in non_stomp]
          dbdocs        = self.get(*non_stomp_ids)
          false_err     = set()

          for notinserted in non_stomp:
            key    = notinserted['_id']
            stored = dbdocs[key]
            if not stored:
              # The value just isn't there.  no idea why this insert failed,
              # but maybe it won't next time around?
              false_err.add(key)
            elif doc_equal(stored, notinserted):
              # The exact value we wanted to write is indeed what is stored.
              # This shouldn't be a problem at all, so we'll just forget that
              # it happened.
              del retry_c[key]
              false_err.add(key)

          non_stomp_ids = set(non_stomp_ids) - false_err
          if non_stomp_ids:
            # At this point, we do have documents we tried to create that were
            # in the database with different values, so we'll yell about it.
            raise BatchCommitFailure("Document creation blocked",
                retry_c, retry_u, retry_d)

      self._creates = retry_c
      self._updates = retry_u
      self._deletes = retry_d
      self.commit(retries-1)

  def _process_updates(self, lresult):
    """Determine the documents we'll be giving to the save_docs as a result of
    update functions being applied to existing or new documents.
    """
    bulk_insert = []
    for key, fns in self._updates.items():
      row = lresult[key]
      try:
        doc = row['doc']
        rev = row['value']['rev']
      except KeyError, err:
        print err
        print row
        assert 'error' in row
        # Nothing found, we use an empty dict
        doc = {}
        rev = None

      for fn in fns:
        doc = copy.deepcopy(doc)
        res = fn(doc)
        if res:
          doc = res

      if not doc:
        continue

      doc['_id'] = row['key']
      if rev:
        doc['_rev'] = rev

      bulk_insert.append(doc)
    return bulk_insert

  def _process_deletes(self, lresult):
    """Determine the deletion documents we'll be giving to the save_docs
    function.
    """
    bulk_insert = []
    for key in self._deletes:
      row = lresult[key]
      try:
        rev = row['value']['rev']
      except KeyError:
        assert 'error' in row
        # No need to delete this one...
        continue
      doc = {
          '_id'      : key,
          '_rev'     : rev,
          '_deleted' : True,
          }
      bulk_insert.append(doc)
    return bulk_insert

  def _process_creates(self, lresult):
    """This doesn't actually return any documents, as the creates are
    unconditionally added to the bulk_insert list in commit().  Instead, it
    just ensures that the __overwrite create docs have proper '_rev' tags
    applied.
    """
    for key, doc in self._creates.items():
      if '__overwrite' not in doc:
        continue

      row = lresult[key]
      try:
        rev = row['value']['rev']
      except KeyError:
        # It doesn't exist? No problem!
        continue
      doc['_rev'] = rev
    return []

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

