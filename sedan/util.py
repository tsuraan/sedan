"""Useful functions to aid the bulk api"""

def doc_equal(d1, d2):
  """Determine whether two couch docs are equal.  This is basically the same
  as dictionary equality, except that we ignore all keys that start with '_'.
  """
  d1p = dict( (key, val) for key, val in d1.items() if not key.startswith('_') )
  d2p = dict( (key, val) for key, val in d2.items() if not key.startswith('_') )
  return d1p == d2p


