"""This is the core of our couch wrapper.  The CouchBatch class defined here
is what end-users can use to efficiently query and update couchdb.
"""
from couchdbkit.exceptions import ResourceConflict
from couchdbkit.exceptions import ResourceNotFound
from couchdbkit.exceptions import BulkSaveError
from functools             import partial
from pprint                import pprint as pp
import copy
import time

from .exceptions import ActionForbidsDocument
from .exceptions import ActionNeedsDocument
from .exceptions import CreateScheduled
from .exceptions import UpdateScheduled
from .exceptions import OverwriteScheduled
from .exceptions import DeleteScheduled
from .promise    import Promise
from .result     import DbFailure
from .result     import DbValue
from .actions    import ReadAction
from .actions    import CreateAction
from .actions    import OverwriteAction
from .actions    import UpdateAction
from .actions    import DeleteAction
from .view       import ViewResults

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

  this logic is enshrined in _set_action far below...
  """
  def __init__(self, couchkit):
    self.__ck = couchkit
    self.clear_cache()
    self._reset()

  @property
  def ck(self):
    return self.__ck

  def new_batch(self):
    """Create a new batch that shares this batch's couch connection, but
    nothing else.  This may be useful when a function passed to this batch's
    update or create methods needs to do some database work, since using this
    batch from a callback would almost certainly break things.

    @return A new CouchBatch instance, sharing this batch's couch connection
    """
    return CouchBatch(self.__ck)

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
      if row.get('doc'):
        _fulfill(self._reads, key, DbValue(row))
        self.__docCache[key] = row
      elif (row.get('error') == 'not_found') or (
          row.get('value', {}).get('deleted')):
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

    if set(['cached']).union(kwargs) != set(['cached']):
      unexpected = list(set(kwargs).difference(['cached']))[0]
      raise TypeError(
          "get() got an unexpected keyword argument '%s'" % unexpected)

    if kwargs.get('cached', True):
      for key in keys:
        try:
          cached = self.__docCache[key]
          promise = Promise(key, lambda: None)
          promise._fulfill(DbValue(cached))
          result[key] = promise
          self._stats['fromcache'] += 1
        except KeyError:
          pass

    notcached = keys - set(result)
    for key in notcached:
      if key in self._reads:
        result[key] = self._reads[key].promise
      else:
        promise = Promise(key, self.do_reads)
	result[key] = promise
	action  = ReadAction(key, promise)
	self._reads[key] = action

    return result

  def view(self, vname, **kwargs):
    """Run a view.  It can be useful to run a view through the couch batch
    rather than directly through the couchkit object because when include_docs
    is given, the docs get added to the batch's cache.

    @param vname  The name of the view to query
    @param kwargs CouchDB view arguments
    @return The result of the view
    """
    rows = self.__ck.view(vname, **kwargs)
    self._stats['read'] += 1
    if kwargs.get('include_docs'):
      return ViewResults(rows, self.__docCache)
    else:
      return rows

  def all(self, **kwargs):
    """Return all the docs in couch.  If include_docs is set, all the docs
    returned from this function will be added to this batch's cache.  This is
    pretty much just a special view, so all the kwargs that a view can take,
    this can take as well.

    @param kwargs CouchDB view arguments
    @return The result of the view
    """
    rows = self.__ck.all_docs(**kwargs)
    self._stats['read'] += 1
    if kwargs.get('include_docs'):
      return ViewResults(rows, self.__docCache)
    else:
      return rows

  def do_writes(self, timelimit=5):
    """Run the current batch of write operations.  This will fulfill all the
    promises we have outstanding on create, overwrite, update, and delete
    operations.

    @param timelimit  How many seconds we have to complete; any commit attempts
                      made after timelimit will be marked as failures
    """
    writes       = self._writes
    self._writes = {}
    start        = time.time()
    promises     = [action.promise for action in writes.values()]

    while True:
      if not writes:
        break
      if time.time() > start + timelimit:
        break

      bulk_write  = {}
      needcurrent = []

      for action in writes.values():
        try:
          if action.docid in self.__docCache:
            doc = action.doc(self.__docCache[action.docid])
          else:
            doc = action.doc()
        except ActionNeedsDocument:
          current = self.get(action.docid)[action.docid]
          needcurrent.append( (current, action) )
          continue
        except ActionForbidsDocument:
          action.promise._fulfill(DbFailure(_make_conflict(action.docid)))
          continue
        except Exception, e:
          action.promise._fulfill(DbFailure(e))
          continue

        if doc:
          bulk_write[action.docid] = doc
        else:
          action.promise._fulfill(DbValue(None))
          del writes[doc.docid]

      for current, action in needcurrent:
        try:
          value = current.value()
          doc = action.doc(value)
        except ActionForbidsDocument:
          action.promise._fulfill(DbFailure(_make_conflict(action.docid)))
          continue
        except Exception, e:
          action.promise._fulfill(DbFailure(e))
          continue

        if doc:
          if doc['_id'] != action.docid:
            assert isinstance(action, CreateAction)
            del writes[action.docid]
            if doc['_id'] in writes:
              action.promise._fulfill(DbFailure(_make_conflict(action.docid)))
              continue
            action = CreateAction(doc['_id'], doc,
                action.promise, action.resolver)
            writes[doc['_id']] = action
          bulk_write[action.docid] = doc
        else:
          action.promise._fulfill(DbValue(None))
          del writes[action.docid]

      try:
        self._stats['write'] += 1
        results = self.__ck.bulk_save(bulk_write.values())
      except BulkSaveError, e:
        results = e.results

      retries = []
      for result in results:
        key = result['id']
        action = writes[key]
        if 'error' in result:
          if action.can_retry:
            retries.append(action)
            self.forget(action.docid)
          else:
            action.promise._fulfill(DbFailure(_make_conflict(result)))
        else:
          action.promise._fulfill(DbValue(result))
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

      writes = dict( (action.docid, action) for action in retries )

    if writes:
      for action in writes.values():
        action.promise._fulfill(DbFailure(_make_conflict(action.docid)))
    return promises


  def create(self, key, document, conflict_fn=None, converter=None):
    """Create a new document.  The promise returned from this will have a
    value that either raises an exception or returns a dictionary with the
    keys "rev" and "id".

    Create will fail if the document already exists in couch; this means that
    if we have the key in our doc cache, or if the document is scheduled to be
    created already, the returned promise will be a failure.

    @param key            The key at which to store the given document
    @param document       The data to store (should be a dictionary)
    @param conflict_fn    A function that accepts the document we're trying to
                          write and the currently-stored document, and which
                          can return a new document to try writing or None to
                          indicate a failure
    @param converter      A function that, once the promise is given a value,
                          will immediately be called with that promise. If it
                          returns a truthy value, that value will be used as
                          the promise's value instead of whatever it would
                          have had.
    @raise ScheduleError  If something is already scheduled to happen for this
                          key
    """
    if key in self.__docCache:
      # We know that the key is in our cache, and thus is known to exist.
      # We'll return a promise that's already set as a failure.
      promise = Promise(key, lambda: None, gotresult_fn=converter)
      promise._fulfill(DbFailure(_make_conflict(key)))
      return promise

    def action_fn(promise):
      return CreateAction(key,
          document,
          promise,
          conflict_resolver=conflict_fn)

    promise = _set_action(self._writes, key, self.do_writes, action_fn,
        converter)
    return promise

  def overwrite(self, key, document, revision=None, converter=None):
    """Stomp over whatever document already exists in the database with the
    given key, or create a new document if needed.  The data given by document
    is what will be in the database once we commit.

    @param key            The key at which to store our data
    @param document       The data to store
    @param revision       The current database rev of the doc, if known
    @param converter      A function that, once the promise is given a value,
                          will immediately be called with that promise. If it
                          returns a truthy value, that value will be used as
                          the promise's value instead of whatever it would
                          have had.
    @raise ScheduleError  If there is already an anything scheduled other than
                          another overwrite.
    """
    promise = _set_action(self._writes, key, self.do_writes,
        partial(OverwriteAction, key, document, revision), converter)
    return promise

  def update(self, key, updatefn, converter=None):
    """Queue up a document update function.  On commit, the document
    associated with given key will be retrieved, and the function will be
    called on the document.  If the document does not exist in the database,
    the returned promise's value will raise a ResourceNotFound exception;
    otherwise, the value will be the update response from the database, which
    is basically a dictionary with a rev and a key (the key you gave this
    function, hopefully)

    The result of the function will be the new value that will be stored (at
    least, given to the other updates for the document).  If the update
    function returns None, it will have no effect on the stored value of the
    document.

    @param key            The key of the document to update
    @param updatefn       The function that will transform the document data
    @param converter      A function that, once the promise is given a value,
                          will immediately be called with that promise. If it
                          returns a truthy value, that value will be used as
                          the promise's value instead of whatever it would
                          have had.
    @raise ScheduleError  If there is already a delete scheduled for this key
    """
    promise = _set_action(self._writes, key, self.do_writes,
        partial(UpdateAction, key, updatefn), converter)
    return promise

  def delete(self, key, converter=None):
    """Delete a document.  The promise returned here will contain a dictionary
    with the keys "rev", "id", and "ok", or it will raise a ResourceNotFound
    exception.

    @param key            The key of the document to delete.
    @param converter      A function that, once the promise is given a value,
                          will immediately be called with that promise. If it
                          returns a truthy value, that value will be used as
                          the promise's value instead of whatever it would
                          have had.
    @raise ScheduleError  If anything other than an overwrite is already
                          scheduled for this key
    """
    promise = _set_action(self._writes, key, self.do_writes,
        partial(DeleteAction, key), converter)
    return promise

def _fulfill(actions, key, result):
  """Complete the promises outstanding for the given document key.

  @param actions  The key -> action dictionary for read or write actions
  @param key      The couch key for the promise
  @param result   The DbResult to record
  """
  if key in actions:
    actions[key].promise._fulfill(result)
    del actions[key]

def _set_action(actions, key, completer_fn, action_fn, converter_fn):
  """Assign (or re-assign) the action for the given key.  This generates a new
  Promise chained to the promise of the already-present action for the key (if
  any).  It then gives that promise to the given action_fn to generate a new
  Action, which it stores in the actions dictionary.  The new promise is
  returned from this function.

  @param actions        The dictionary of key -> action
  @param key            The key we want to set the action for
  @param completer_fn   The function the promise needs to call when its value
                        is requested
  @param action_fn      The function that will give us an Action
  @param converter_fn   A function that, once the promise is given a value,
                        will immediately be called with that promise. If it
                        returns a truthy value, that value will be used as
                        the promise's value instead of whatever it would
                        have had.
  @return             A new promise object to give to the user
  """
  try:
    existing = actions[key]
    prev_promise = existing.promise
  except KeyError:
    existing     = None
    prev_promise = None
  
  promise = Promise(key, completer_fn, prev_promise, converter_fn)
  new     = action_fn(promise)

  if isinstance(existing, CreateAction):
    if isinstance(new, CreateAction):
      raise CreateScheduled
    elif isinstance(new, OverwriteAction):
      raise CreateScheduled
    elif isinstance(new, UpdateAction):
      new, promise = _update_doc(key, new, existing, promise)
    elif isinstance(new, DeleteAction):
      raise CreateScheduled
  elif isinstance(existing, OverwriteAction):
    if isinstance(new, CreateAction):
      raise OverwriteScheduled
    elif isinstance(new, OverwriteAction):
      # This will just do the natural thing, which is the new overwrite taking
      # precedence; the promises are already chained
      pass
    elif isinstance(new, UpdateAction):
      new, promise = _update_doc(key, new, existing, promise)
    elif isinstance(new, DeleteAction):
      # This is the natural action; new is already a delete, and the promises
      # are already chained
      pass
  elif isinstance(existing, UpdateAction):
    if isinstance(new, CreateAction):
      raise UpdateScheduled
    elif isinstance(new, OverwriteAction):
      raise UpdateScheduled
    elif isinstance(new, UpdateAction):
      # Compose the new update function with the existing one, create a new
      # update action from that
      origNew = new
      def composed(doc):
        fromExisting = existing.doc({'doc':doc})
        fromNew = origNew.doc({'doc':fromExisting})
        return fromNew
      new = UpdateAction(key, composed, promise)
    elif isinstance(new, DeleteAction):
      raise UpdateScheduled
  elif isinstance(existing, DeleteAction):
    # The chart for existing = delete is pretty simple...
    raise DeleteScheduled

  actions[key] = new
  return promise

def _update_doc(key, new, existing, promise):
  """Pass the doc to be created by the existing through the function stored in
  the new, wrap the result in a create.  If the update function throws an
  exception, we will make a failed promise and throw away the update; if the
  update function returns None, we will make a successful promise with the
  value of None and throw away the update
  """
  assert isinstance(new, UpdateAction)
  assert (isinstance(existing, CreateAction) 
      or isinstance(existing, OverwriteAction) )

  try:
    updated = new.doc({'doc':existing.doc()})
  except Exception, e:
    new     = existing
    promise = Promise(key, lambda: None)
    promise._fulfill(DbFailure(e))
  else:
    if updated:
      if isinstance(existing, CreateAction):
        new = CreateAction(existing.docid, updated, promise, existing.resolver)
      else:
        new = OverwriteAction(existing.docid, updated, None, promise)
    else:
      new     = existing
      promise = Promise(key, lambda: None)
      promise._fulfill(DbValue(None))
  return new, promise

def _make_conflict(key):
  """Generate a ResourceConflict exception object for the given key"""
  conflict = ResourceConflict({
    'id'     : key,
    'error'  : 'conflict',
    'reason' : 'Document update conflict.',
    })
  return conflict

