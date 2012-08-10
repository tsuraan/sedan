"""This is the definition of the Promise class, which is what all couch
bulk operations return.
"""
from .result import DbFailure
from .result import DbResult
from .result import DbValue
import copy

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
  def __init__(self, key, completer_fn, previous=None, gotresult_fn=None):
    self.__completer = completer_fn
    self.__gotresult = gotresult_fn
    self.__result    = None
    self.__key       = key
    if previous:
      assert isinstance(previous, Promise)
      self.__peers = previous.__peers
      self.__peers.add(self)
    else:
      self.__peers = set([self])

  @property
  def key(self):
    return self.__key

  def value(self):
    if not self.__result:
      self.__completer()

    assert isinstance(self.__result, DbResult)
    if isinstance(self.__result, DbFailure):
      raise self.__result.value
    elif isinstance(self.__result, DbValue):
      return copy.deepcopy(self.__result.value)
    raise RuntimeError("Unexpected result type: %s" % type(result))

  def _fulfill(self, result, __recurse=True):
    if self.__result:
      raise RuntimeError("Promise already completed")
    assert isinstance(result, DbResult)
    if __recurse:
      for promise in self.__peers:
        try:
          promise._fulfill(result, False)
        except:
          pass
      return

    self.__result = result
    if self.__gotresult:
      try:
        newvalue = DbValue(self.__gotresult(self))
      except Exception, e:
        newvalue = DbFailure(e)
      if newvalue:
        self.__result = newvalue

