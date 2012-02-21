from .batch import ResourceNotFound
from .batch import CouchBatch

import unittest
import copy

def canned(testCase):
  testCase.a = {
      'alpha' : 1,
      'beta'  : 2,
      }
  testCase.b = {
      'alpha' : 2,
      'beta'  : 3,
      'gamma' : 4,
      }
  testCase.c = {
      'alpha' : 3,
      'beta'  : 1,
      'delta' : 'hi there',
      }

  testCase.ck = FakeCK(docs={
    'a' : copy.deepcopy(testCase.a),
    'b' : copy.deepcopy(testCase.b),
    'c' : copy.deepcopy(testCase.c),
    })
  testCase.batch = CouchBatch(testCase.ck)

class TestGet(unittest.TestCase):
  def setUp(self):
    canned(self)

  def assertReadStat(self, num):
    self.assertEqual(self.batch._stats['read'], num)

  def _cleanDoc(self, doc):
    return dict( ( key, doc[key] ) for key in doc if not key.startswith('_') )

  def testSingle(self):
    """Getting one existing doc works"""
    b1 = self.batch.get('b')['b']
    self.assertReadStat(0)

    b1 = b1.value()['doc']
    self.assertReadStat(1)

    b = self._cleanDoc(b1)
    self.assertEqual(b, self.b)

    # And, make sure we read from cache
    b2 = self.batch.get('b')['b']
    b2 = b2.value()['doc']
    self.assertReadStat(1)

    self.assertEqual(b1, b2)

  def testBatch(self):
    """Multi-get is sane"""
    d1 = self.batch.get('a', 'c')
    b1 = self.batch.get('b')
    self.assertReadStat(0)

    a1 = d1['a'].value()['doc']
    b1 = b1['b'].value()['doc']
    c1 = d1['c'].value()['doc']
    self.assertReadStat(1)

    a = self._cleanDoc(a1)
    b = self._cleanDoc(b1)
    c = self._cleanDoc(c1)

    self.assertEqual(a, self.a)
    self.assertEqual(b, self.b)
    self.assertEqual(c, self.c)

    d2 = self.batch.get('a', 'c')
    b2 = self.batch.get('b', cached=False)
    self.assertReadStat(1)

    a2 = d2['a'].value()['doc']
    self.assertReadStat(1)
    b2 = b2['b'].value()['doc']
    self.assertReadStat(2)
    c2 = d2['c'].value()['doc']
    self.assertReadStat(2)

    self.assertEqual(a1, a2)
    self.assertEqual(b1, b2)
    self.assertEqual(c1, c2)

  def testRepeat(self):
    """Asking for the same key twice does the right thing"""
    d = self.batch.get('a', 'c')
    e = self.batch.get('b', 'c')
    self.assertReadStat(0)

    a1 = d['a'].value()['doc']
    c1 = d['c'].value()['doc']
    b1 = e['b'].value()['doc']
    c2 = e['c'].value()['doc']
    self.assertReadStat(1)

    self.assertEqual(self.a, self._cleanDoc(a1))
    self.assertEqual(self.b, self._cleanDoc(b1))
    self.assertEqual(self.c, self._cleanDoc(c1))
    self.assertEqual(self.c, self._cleanDoc(c2))


  def testMissing(self):
    """Missing data produces ResourceNotFound exceptions"""
    d1 = self.batch.get('a', 'c', 'missing-key')
    self.assertReadStat(0)

    a1 = d1['a'].value()['doc']
    self.assertReadStat(1)
    c1 = d1['c'].value()['doc']
    self.assertReadStat(1)
    self.assertRaises(ResourceNotFound, d1['missing-key'].value)
    self.assertReadStat(1)

    self.assertEqual(self._cleanDoc(a1), self.a)
    self.assertEqual(self._cleanDoc(c1), self.c)

    # Ensure that re-getting value keeps raising the exception
    self.assertRaises(ResourceNotFound, d1['missing-key'].value)
    self.assertReadStat(1)

class TestCreate(unittest.TestCase):
  pass

class FakeResults(object):
  def __init__(self, results):
    self.__results = results

  def all(self):
    return copy.deepcopy(self.__results)

  def __iter__(self):
    for res in self.__results:
      yield copy.deepcopy(res)

  def __len__(self):
    return len(self.__results__)

class FakeCK(object):
  """This class simulates enough of the couchkit interface to test out our
  CouchBatch class.  It's not good or pretty, but it's sure quicker than
  creating, populating, and destroying couchdb databases for every test.
  """
  def __init__(self, docs={}, views={}):
    """Create a fake couchdbkit database instance.  

    @param docs   A dictionary mapping keys to documents
    @param views  A dictionary mapping views to their results (not working yet)
    """
    docs = copy.deepcopy(docs)
    for key, doc in docs.items():
      doc['_rev'] = 0
      doc['_id']  = key

    self.__docs = docs


  def all_docs(self, keys=None, include_docs=False, skip=0, limit=None):
    if not keys:
      keys = sorted(self.__docs.keys())
    
    keys = keys[skip:]
    if limit:
      keys = keys[:limit]

    results = []
    for key in keys:
      try:
        doc = self.__docs[key]
        result = {
            'id' : key,
            'key' : key,
            'value' : {'rev' : doc['_rev']}
            }
        if include_docs:
          result['doc'] = copy.deepcopy(doc)
        results.append(result)
      except KeyError:
        results.append( {'key' : key, 'error' : 'not_found'} )

    return FakeResults(results)

if __name__ == "__main__":
  unittest.main()

