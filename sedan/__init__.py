def doc_equal(d1, d2):
  """Determine whether two couch docs are equal.  This is basically the same
  as dictionary equality, except that we ignore all keys that start with '_'.
  """
  d1p = dict( (key, val) for key, val in d1.items() if not key.startswith('_') )
  d2p = dict( (key, val) for key, val in d2.items() if not key.startswith('_') )
  return d1p == d2p

class DbResult(object):
  """This is the result of a document fetch, update, or create.  It is either
  a failure or a success message.
  """
  @property
  def successful(self):
    raise NotImplementedError
  @property
  def value(self):
    raise NotImplementedError

class DbFailure(DbResult):
  """Record a failure to interact with a given document.  The value stored
  here must be an exception, since DbValues will raise it when it is seen.
  """
  def __init__(self, error):
    self.__value = err
  @property
  def successful(self):
    return False
  @property
  def value(self):
    return copy.deepcopy(self.__value)

class DbValue(DbResult):
  """Record a succesful value returned by the database."""
  def __init__(self, value):
    self.__value = value
  @property
  def successful(self):
    return True
  @property
  def value(self):
    return copy.deepcopy(self.__value)

class Promise(object):
  """This is what is returned by the single-document database interaction
  functions of the CouchBatch object.  At the time of Promise construction, we
  have not made a request to the database.  Instead, the database work is
  delayed until this object's ".value" method is called.  This allows us to
  batch up a bunch of operations into a single couch request.

  A Promise's value is accessed through its ".value" method.  As this
  represents the result from the database, it will either return a dictionary
  or it will raise an exception.
  """
  def __init__(self, batch):
    self.__batch  = batch
    self.__result = None

  def value(self):
    if not self.__result:
      self.__batch.commit(self)

    assert isinstance(self.__result, DbResult)
    if isinstance(self.__result, DbFailure):
      raise self.__result.value
    elif isinstance(self.__result, DbValue):
      return self.__result.value
    raise RuntimeError("Unexpected result type: %s" % type(result))


class BatchJobNeedsDocument(Exception):
  """Raised by a batch job's ".doc" method when it is called without a
  document, and the job needs a document to operate on.
  """

class BatchJobForbidsDocument(Exception):
  """Raised by a batch job's ".doc" method when it is called with a document,
  and the batch job needs the document to not exist.
  """

class BatchJob(object):
  """Object representing a job that needs to be done.  Current subclasses of
  this are create, replace, update, and delete jobs.
  """
  def __init__(self, docid, promise):
    self.__promise = promise

  def promise(self):
    """Get the promise that was associated with this job"""
    return self.__promise

  @property
  def docid(self):
    return self.__id

  def doc(self, current=None):
    """Get the document that needs to be batch-submitted to the database.  If
    this method returns None, the job will be skipped without any error.

    The given "current" parameter will be None if we don't know what the
    document is, {} if the document doesn't yet exist in the database, and
    some other dictionary if we just grabbed the doc from the database.
    """
    raise NotImplementedError

  @property
  def docid(self):
    """Get the couch id for the document this job will be applying to."""
    raise NotImplementedError

class CreateJob(BatchJob):
  def __init__(self, docid, doc, promise):
    BatchJob.__init__(self, docid, promise)
    self.__doc = doc

  def doc(self, current=None):
    if current is not None:
      raise BatchJobForbidsDocument
    return self.__doc

class ReplaceJob(BatchJob):
  def __init__(self, docid, doc, promise):
    BatchJob.__init__(self, docid, promise)
    self.__doc = doc

  def doc(self, current=None):
    return self.__doc

class UpdateJob(BatchJob):
  def __init__(self, docid, fn, promise):
    BatchJob.__init__(self, docid, promise)
    self.__fn = fn

  def doc(self, current=None):
    if current is None:
      raise BatchJobNeedsDocument
    return self.__fn(current)

class DeleteJob(BatchJob):
  def __init__(self, docid, promise):
    BatchJob.__init__(self, docid, promise)

  def doc(self, current=None):
    if current is None:
      raise BatchJobNeedsDocument
    if not current:
      return None
    doc = {
        '_id'      : doc['_id'],
        '_rev'     : doc['_rev'],
        '_deleted' : True,
        }
    return doc

class CouchBatch(object):
  """This class allows the user to queue up numerous couch operations which
  will be submitted as a single save_docs call when the commit() method is
  called.  The only time this ever touches couchdb is when the commit() method
  is called; otherwise, this is completely local.
  """
  def __init__(self, couchkit):
    self.__ck = conns.couchkit
    self._reset()
    self.clear_cache()

  def clear_cache(self):
    self.__docCache = {}

  def forget(self, key):
    """Remove the given key from our cache.  Mostly useful for the commit
    method, which needs to drop from our cache any docs that have had update
    conflicts.  Possibly useful in other cases though.
    """
    try:
      del self.__docCache[key]
    except KeyError:
      pass

  def get(self, *keys):
    """Get the desired documents from couch.  This will return a dictionary of
    key -> dictionary.  If a key could not be found, it will be in the
    dictionary, pointing at None.

    Notice this returns the actual couch response dictionaries, so the typical
    keys are 'rev', 'doc', etc.  The data you're looking for is probably under
    the key of 'doc', unless you want the doc's revision or something else
    meta about the document.

    @param keys   The keys for the documents we want to retreive
    """
    keys = set(keys)
    result = {}
    for key in keys:
      try:
        result[key] = copy.deepcopy(self.__docCache[key])
        self._stats['fromcache'] += 1
      except KeyError:
        pass

    keys = keys - set(result)
    if keys:
      rows = self.__ck.all_docs(keys=list(keys), include_docs=True)
      self._stats['read'] += 1
      for row in rows:
        if 'doc' in row:
          self.__docCache[row['key']] = row

      for key in keys:
        try:
          result[key] = copy.deepcopy(self.__docCache[key])
        except KeyError:
          result[key] = None

    return result

  def __getitem__(self, key):
    """Gets the doc (actual doc, not raw couchdb doc) referenced by the given
    key, or raises KeyError if the key was not found in the database.  If key
    is a non-string iterable, then this will return the docs in the order of
    the given keys, with None values for keys that were not found.
    """
    if isinstance(key, basestring):
      record = self.get(key)[key]
      if not record:
        raise KeyError
      return record['doc']

    records = self.get(*key)
    result  = []
    for k in key:
      record = records[k]
      if not record:
        result.append(None)
      else:
        result.append(record['doc'])
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
      for row in rows:
        doc = row['doc']
        rev = doc['_rev']
        did = doc['_id']
        if did in self.__docCache:
          continue
        self.__docCache[did] = {
            'doc' : doc,
            'id'  : did,
            'key' : did,
            'value' : {'rev':rev},
            }
    return rows

  def _reset(self):
    """Setup the operations"""
    self._creates   = {}
    self._updates   = {}
    self._stats     = { 'read' : 0, 'write' : 0, 'fromcache' : 0 }
    self._deletes   = set()

  def _sanity(self):
    """We do not allow the same keys to appear in any two of our operation
    maps.  Ensure that that's the case.
    """
    c = set(self._creates)
    u = set(self._updates)
    d = self._deletes
    assert len(c.intersection(u).intersection(d)) == 0

  def create(self, key, document):
    """Queue up a document creation.  This will fail immediately if we've
    already been told to create this document.  If the key is scheduled for
    updating, this will fail because the doc would already exist.  If the key
    is scheduled for deletion, the delete will no longer be scheduled.

    @param key      The key at which to store the given document
    @param document The data to store (should be a dictionary)
    """
    if key in self._creates:
      if document == self._creates[key]:
        return
      raise ValueError("Attempt to re-create document %s" % key)

    if key in self._updates:
      # the update will cause the doc to be created empty, so this create is
      # invalid
      raise ValueError("Attempt to create a doc that is being updated")

    if key in self._deletes:
      # create after delete? we'll skip the delete
      self._deletes.remove(key)
      document['__overwrite'] = True

    self._creates[key] = document
    self._sanity()

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

