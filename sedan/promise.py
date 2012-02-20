"""This is the definition of the Promise class, which is what all couch
bulk operations return.
"""

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
  def __init__(self, completer_fn):
    self.__completer = completer_fn
    self.__result    = None

  def value(self):
    if not self.__result:
      self.completer_fn()

    assert isinstance(self.__result, DbResult)
    if isinstance(self.__result, DbFailure):
      raise self.__result.value
    elif isinstance(self.__result, DbValue):
      return self.__result.value
    raise RuntimeError("Unexpected result type: %s" % type(result))

  def _fulfill(self, result):
    if self.__result:
      raise RuntimeError("Promise already completed")
    assert isinstance(result, DbResult)
    self.__result = result

