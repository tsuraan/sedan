"""Possible results from a database query."""
import copy

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
    self.__value = error
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

