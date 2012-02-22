"""Exceptions used by the bulk API"""

class BatchJobNeedsDocument(Exception):
  """Raised by a batch job's ".doc" method when it is called without a
  document, and the job needs a document to operate on.
  """

class BatchJobForbidsDocument(Exception):
  """Raised by a batch job's ".doc" method when it is called with a document,
  and the batch job needs the document to not exist.
  """

