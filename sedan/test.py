from .batch import ResourceNotFound
from .batch import CouchBatch

from couchdbkit.exceptions import BulkSaveError
import unittest
import random
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

class BaseTest(unittest.TestCase):
  def setUp(self):
    canned(self)

  def assertReadStat(self, num):
    self.assertEqual(self.batch._stats['read'], num)

  def assertWriteStat(self, num):
    self.assertEqual(self.batch._stats['write'], num)

  def cleanDoc(self, doc):
    return dict( ( key, doc[key] ) for key in doc if not key.startswith('_') )

class TestGet(BaseTest):
  def testSingle(self):
    """Getting one existing doc works"""
    b1 = self.batch.get('b')['b']
    self.assertReadStat(0)

    b1 = b1.value()['doc']
    self.assertReadStat(1)

    b = self.cleanDoc(b1)
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

    a = self.cleanDoc(a1)
    b = self.cleanDoc(b1)
    c = self.cleanDoc(c1)

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

    self.assertEqual(self.a, self.cleanDoc(a1))
    self.assertEqual(self.b, self.cleanDoc(b1))
    self.assertEqual(self.c, self.cleanDoc(c1))
    self.assertEqual(self.c, self.cleanDoc(c2))


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

    self.assertEqual(self.cleanDoc(a1), self.a)
    self.assertEqual(self.cleanDoc(c1), self.c)

    # Ensure that re-getting value keeps raising the exception
    self.assertRaises(ResourceNotFound, d1['missing-key'].value)
    self.assertReadStat(1)

class TestCreate(BaseTest):
  def testSuccess(self):
    """Making a single document works"""
    promise = self.batch.create('d', {'foo':'bar','bar':'baz'})
    self.assertReadStat(0)
    self.assertWriteStat(0)

    self.assertEqual(promise.value(), {'rev':1, 'id':'d'})
    self.assertReadStat(0)
    self.assertWriteStat(1)

    promise = self.batch.get('d')['d']
    self.assertEqual(self.cleanDoc(promise.value()['doc']),
        {'foo':'bar','bar':'baz'})
    self.assertReadStat(1)
    self.assertWriteStat(1)

class TestCouchKit(unittest.TestCase):
  def normalize_revision(self, row):
    if isinstance(row.get('value',{}).get('rev'), basestring):
      row['value']['rev'] = int(row['value']['rev'].split('-')[0])
    if isinstance(row.get('rev'), basestring):
      row['rev'] = int(row['rev'].split('-')[0])
    if isinstance(row.get('doc',{}).get('_rev'), basestring):
      row['doc']['_rev'] = int(row['doc']['_rev'].split('-')[0])

  def setUp(self):
    canned(self)

  def testGet(self):
    """Simple get works"""
    result = sorted(self.ck.all_docs(keys=['a','c']).all())
    map(self.normalize_revision, result)

    self.assertEqual(result, [
      {'key':'a', 'id':'a','value':{'rev':1}},
      {'key':'c', 'id':'c','value':{'rev':1}},
      ])
    return

  def testGetMissing(self):
    """Get with a missing value returns the expected data"""
    result = sorted(self.ck.all_docs(keys=['a','c','e']).all())
    map(self.normalize_revision, result)

    self.assertEqual(result, [
      {'error': 'not_found', 'key':'e'},
      {'key':'a', 'id':'a','value':{'rev':1}},
      {'key':'c', 'id':'c','value':{'rev':1}},
      ])
    return

  def testCreate(self):
    """Simple document creation works"""
    result = self.ck.bulk_save([{'_id':'d', 'alpha':12}])
    map(self.normalize_revision, result)
    self.assertEqual(result, [{'rev':1, 'id':'d'}])

    result = self.ck.all_docs(keys=['d'], include_docs=True)
    map(self.normalize_revision, result)

    for idx, row in enumerate(result):
      self.assertEqual(idx, 0)
      self.assertEqual(row, {
        'doc':{'_id':'d','_rev':1,'alpha':12},
        'id':'d',
        'key':'d',
        'value':{'rev':1},
        })
    return

  def testConflict(self):
    """Make sure conflicts raise exceptions"""
    with self.assertRaises(BulkSaveError) as r:
      self.ck.bulk_save([{'_id':'a', 'alpha':'dog'},
        {'_id':'d', 'alpha':12}])

    map(self.normalize_revision, r.exception.results)
    self.assertEqual(sorted(r.exception.results),
        [ {'id':'d', 'rev':1},
          { 'error': 'conflict',
            'id':'a',
            'reason':'Document update conflict.',
            },
          ])

    result = self.ck.all_docs(keys=['a','d'], include_docs=True).all()
    map(self.normalize_revision, result)
    self.assertEqual(result,
        [{ 'doc': {'_id':'a',
                   '_rev':1,
                   'alpha':1,
                   'beta':2,
                   },
           'id':'a',
           'key':'a',
           'value':{'rev':1}},
         { 'doc': {'_id':'d',
                   '_rev':1,
                   'alpha':12},
           'id':'d',
           'key':'d',
           'value':{'rev':1}}])



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
      doc['_rev'] = 1
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

  def bulk_save(self, docs):
    """Store the docs as given, except for the docs that have '_deleted':True;
    those docs will indicate that a key needs deleting.  For all docs, the
    '_rev' key of the doc must match the stored rev, or an exception is
    thrown.
    """
    stored    = self.__docs
    results = []
    for doc in docs:
      rev = doc.get('_rev')
      key = doc.get('_id', self.randid())

      # First we'll make sure we don't have any revision issues
      conflicted = False
      conflict = {
          'error'  : 'conflict',
          'id'     : key,
          'reason' : 'Document update conflict.',
          }
      if (key in stored) and (stored[key]['_rev'] != rev) or (
          rev and (key not in stored)):
        results.append(conflict)
        continue

      # The next thing to look for is a delete
      if doc.get('_deleted') == True:
        try:
          found = stored[key]
          del stored[key]
          results.append({'rev': found['_rev']+1, 'id':key})
          continue
        except KeyError:
          results.append(conflict)
          continue

      # Ok, the revision is good, and it's not a delete, so store the doc
      doc = copy.deepcopy(doc)
      doc['_id']  = key
      doc['_rev'] = stored.get(key,{}).get('_rev', 0) + 1
      stored[key] = doc
      results.append({'rev':doc['_rev'], 'id':key})

    errors = [r for r in results if r.get('error')]
    if errors:
      raise BulkSaveError(errors, results)
    return results

  def randid(self):
    """Generate a random docid"""
    return ''.join(chr(random.randint(0,255)) for _ in range(16)).encode('hex')

if __name__ == "__main__":
  unittest.main()

