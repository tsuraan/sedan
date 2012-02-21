"""Different jobs that we enqueue into our job queues.  These will generally
not be needed by the end-user.
"""

class BatchJob(object):
  """Object representing a job that needs to be done.  Current subclasses of
  this are create, replace, update, and delete jobs.
  """
  def __init__(self, docid, promise):
    self.__id      = docid
    self.__promise = promise

  @property
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

class ReadJob(BatchJob):
  def __init__(self, docid, promise):
    BatchJob.__init__(self, docid, promise)

class CreateJob(BatchJob):
  def __init__(self, docid, doc, promise):
    BatchJob.__init__(self, docid, promise)
    self.__doc = doc

  def doc(self, current=None):
    if current is not None:
      raise BatchJobForbidsDocument
    return self.__doc

class ReplaceJob(BatchJob):
  def __init__(self, docid, doc, revision, promise):
    BatchJob.__init__(self, docid, promise)
    self.__rev = revision
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

