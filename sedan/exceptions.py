"""Exceptions used by the bulk API"""

class ScheduleError(Exception):
  """Base class of exceptions raised when attempting to layer one action over
  an existing one.
  """

class CreateScheduled(ScheduleError):
  """Attempted to layer an illegal action over a create action."""

class UpdateScheduled(ScheduleError):
  """Attempted to layer an illegal action over an update action."""

class OverwriteScheduled(ScheduleError):
  """Attempted to layer an illegal action over an overwrite action."""

class DeleteScheduled(ScheduleError):
  """Attempted to layer an illegal action over a delete action."""

class BatchJobNeedsDocument(Exception):
  """Raised by a batch job's ".doc" method when it is called without a
  document, and the job needs a document to operate on.
  """

class BatchJobForbidsDocument(Exception):
  """Raised by a batch job's ".doc" method when it is called with a document,
  and the batch job needs the document to not exist.
  """

