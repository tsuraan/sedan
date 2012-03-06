from .batch import ResourceNotFound
from .batch import ResourceConflict
from .batch import CreateScheduled
from .batch import UpdateScheduled
from .batch import OverwriteScheduled
from .batch import DeleteScheduled
from .batch import CouchBatch

from couchdbkit.exceptions import BulkSaveError
from pprint import pprint as pp
import couchdbkit
import unittest
import numbers
import random
import copy
import sys

if '--real-couch' in sys.argv:
  print 'using real couch'
  sys.argv.remove('--real-couch')
  print sys.argv
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

    testCase.ck = couchdbkit.Server().create_db('testdb')
    acopy = copy.deepcopy(testCase.a)
    testCase.ck['a'] = acopy
    testCase.ck['b'] = copy.deepcopy(testCase.b)
    testCase.ck['c'] = copy.deepcopy(testCase.c)
    testCase.batch = CouchBatch(testCase.ck)
    testCase.a_rev = acopy['_rev']

  def teardown(testCase):
    couchdbkit.Server().delete_db('testdb')

else:
  def canned(testCase):
    testCase.a = {
        'alpha' : 1,
        'beta'  : 2,
        }
    testCase.a_rev = 1
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

  def teardown(testCase):
    pass

class BaseTest(unittest.TestCase):
  def setUp(self):
    self.addCleanup(teardown, self)
    canned(self)

  def assertReadStat(self, num):
    self.assertStats(read=num)

  def assertWriteStat(self, num):
    self.assertStats(write=num)

  def assertStats(self, read=None, write=None):
    if read is not None:
      self.assertEqual(self.batch._stats['read'], read)
    if write is not None:
      self.assertEqual(self.batch._stats['write'], write)

  def cleanDoc(self, doc):
    return dict( ( key, doc[key] ) for key in doc if not key.startswith('_') )

  def nextRev(self, currev):
    if isinstance(currev, numbers.Integral):
      return currev+1
    elif isinstance(currev, basestring):
      first, rest = currev.split('-', 1)
      return str(int(first)+1) + '-' + rest
    else:
      raise ValueError("Cannot get next for %s of type %s" % (currev,
        type(currev)))

  def normalize_revision(self, row):
    if isinstance(row.get('value',{}).get('rev'), basestring):
      row['value']['rev'] = int(row['value']['rev'].split('-')[0])
    if isinstance(row.get('rev'), basestring):
      row['rev'] = int(row['rev'].split('-')[0])
    if isinstance(row.get('doc',{}).get('_rev'), basestring):
      row['doc']['_rev'] = int(row['doc']['_rev'].split('-')[0])

    return row

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
    self.assertStats(read=0, write=0)

    self.assertEqual(
        self.normalize_revision(promise.value()),
        {'rev':1, 'id':'d'})
    self.assertStats(read=0, write=1)

    cached = self.batch.get('d')['d']
    self.assertEqual(self.cleanDoc(cached.value()['doc']),
        {'foo':'bar','bar':'baz'})
    self.assertStats(read=0, write=1)

    fresh = self.batch.get('d', cached=False)['d']
    self.assertEqual(self.cleanDoc(fresh.value()),
        cached.value())
    self.assertStats(read=1, write=1)

  def testConflict(self):
    conflict = self.batch.create('a', {'foo':'bar'})
    succeed  = self.batch.create('d', {'bar':'baz'})
    self.assertStats(read=0, write=0)

    self.assertRaises(ResourceConflict, conflict.value)
    self.assertStats(read=0, write=1)

    self.assertEqual(
        self.cleanDoc(self.normalize_revision(succeed.value())),
        {'rev':1,'id':'d'})
    self.assertStats(read=0, write=1)

    cached = self.batch.get('d')['d']
    self.assertEqual(self.cleanDoc(cached.value()['doc']),
        {'bar':'baz'})
    self.assertStats(read=0, write=1)

    fresh = self.batch.get('d', cached=False)['d']
    self.assertEqual(self.cleanDoc(fresh.value()),
        cached.value())
    self.assertStats(read=1, write=1)

  def testConflictInCache(self):
    """Make sure that when the batch knows about a document it can generate a
    failure without reading or writing to the database.
    """
    result = self.batch.get('a')['a']
    self.assertEqual(self.cleanDoc(result.value()['doc']), self.a)
    self.assertStats(read=1, write=0)

    conflict = self.batch.create('a', {'foo':'bar'})
    self.assertStats(read=1, write=0)

    self.assertRaises(ResourceConflict, conflict.value)
    self.assertStats(read=1, write=0)

  def testCacheConflictWithSuccess(self):
    """A mixture of cached conflicts, insert conflicts, and successes works.
    """
    result = self.batch.get('a')['a']
    self.assertEqual(self.cleanDoc(result.value()['doc']), self.a)
    self.assertStats(read=1, write=0)

    cache_conflict = self.batch.create('a', {'foo':'bar'})
    lazy_conflict  = self.batch.create('b', {'bar':'baz'})
    success        = self.batch.create('d', {'baz':'thinger'})
    self.assertStats(read=1, write=0)

    self.assertRaises(ResourceConflict, cache_conflict.value)
    self.assertRaises(ResourceConflict, lazy_conflict.value)
    self.assertEqual(
        self.normalize_revision(success.value()),
        {'id':'d', 'rev':1})

    self.assertStats(read=1, write=1)

class TestOverwrite(BaseTest):
  def testSquishCached(self):
    """We stomp over existing values (cached variation)"""
    p1 = self.batch.get('a')['a']
    self.assertStats(read=0, write=0)
    v1 = p1.value()
    self.assertStats(read=1, write=0)
    self.assertEqual(self.cleanDoc(v1['doc']), self.a)

    p2 = self.batch.overwrite('a', {'moo':'cow'})
    self.assertStats(read=1, write=0)
    p2.value()
    self.assertStats(read=1, write=1)

    p3 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(self.cleanDoc(p3.value()['doc']), {'moo':'cow'})
    self.assertStats(read=1, write=1)

    p4 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(p3.value(), p4.value())
    self.assertStats(read=2, write=1)

  def testSquishUncached(self):
    """We stomp over existing values (uncached variation)"""
    p1 = self.batch.overwrite('a', {'moo':'cow'})
    self.assertStats(read=0, write=0)
    p1.value()
    self.assertStats(read=1, write=2)

    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=2)
    self.assertEqual(self.cleanDoc(p2.value()['doc']), {'moo':'cow'})
    self.assertStats(read=1, write=2)

    p3 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=2)
    self.assertEqual(p2.value(), p3.value())
    self.assertStats(read=2, write=2)

  def testOverwriteNothing(self):
    """We can overwrite when there is no existing value"""
    p1 = self.batch.overwrite('d', {'moo':'cow'})
    self.assertStats(read=0, write=0)
    p1.value()
    self.assertStats(read=0, write=1)

    p2 = self.batch.get('d')['d']
    self.assertStats(read=0, write=1)
    self.assertEqual(self.cleanDoc(p2.value()['doc']), {'moo':'cow'})
    self.assertStats(read=0, write=1)

    p3 = self.batch.get('d', cached=False)['d']
    self.assertStats(read=0, write=1)
    self.assertEqual(p2.value(), p3.value())
    self.assertStats(read=1, write=1)

  def testOverwriteCorrectRevision(self):
    """An overwrite given the correct revision is pretty efficient"""
    p1 = self.batch.overwrite('a', {'moo':'cow'}, revision=self.a_rev)
    self.assertStats(read=0, write=0)
    p1.value()
    self.assertStats(read=0, write=1)

    p2 = self.batch.get('a')['a']
    self.assertStats(read=0, write=1)
    self.assertEqual(self.cleanDoc(p2.value()['doc']), {'moo':'cow'})
    self.assertStats(read=0, write=1)

    p3 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=0, write=1)
    self.assertEqual(p2.value(), p3.value())
    self.assertStats(read=1, write=1)

  def testOverwriteWrongRevision(self):
    """An overwrite given the wrong revision still works"""
    p1 = self.batch.overwrite('a', {'moo':'cow'},
        revision=self.nextRev(self.a_rev))
    self.assertStats(read=0, write=0)
    p1.value()
    self.assertStats(read=1, write=2)

    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=2)
    self.assertEqual(self.cleanDoc(p2.value()['doc']), {'moo':'cow'})
    self.assertStats(read=1, write=2)

    p3 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=2)
    self.assertEqual(p2.value(), p3.value())
    self.assertStats(read=2, write=2)

class TestDelete(BaseTest):
  def testOne(self):
    """Deleting a value works"""
    promise = self.batch.delete('a')
    self.assertStats(read=0, write=0)

    self.assertEqual(
        self.normalize_revision(promise.value()),
        {'id':'a','rev':2})
    self.assertStats(read=1, write=1)

    promise = self.batch.get('a')['a']
    self.assertRaises(ResourceNotFound, promise.value)
    self.assertStats(read=2, write=1)

  def testFailure(self):
    """Deleting missing docs gives errors"""
    success = self.batch.delete('a')
    failure = self.batch.delete('d')
    self.assertStats(read=0, write=0)

    self.assertEqual(
        self.normalize_revision(success.value()),
        {'id':'a','rev':2})
    self.assertRaises(ResourceNotFound, failure.value)
    self.assertStats(read=1, write=1)

    promises = self.batch.get('a', 'd')
    self.assertRaises(ResourceNotFound, promises['a'].value)
    self.assertStats(read=2, write=1)

    self.assertRaises(ResourceNotFound, promises['d'].value)
    self.assertStats(read=2, write=1)

class TestOverlaps(BaseTest):
  """Test that the document behaviour of scheduling actions over other actions
  is followed by the code
  """
  def upd(self, update):
    def updater(doc):
      doc.update(update)
      return doc
    return updater

  def testCreateCreate(self):
    """Create followed by Create raises CreateScheduled"""
    p1 = self.batch.create('d', {'a':1})
    self.assertRaises(CreateScheduled, self.batch.create, 'd', {'a':2})
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=0, write=1)

    p2 = self.batch.get('d', cached=False)['d']
    self.assertStats(read=0, write=1)

    self.assertEqual(self.cleanDoc(p2.value()['doc']), {'a':1})
    self.assertStats(read=1, write=1)

  def testCreateOverwrite(self):
    """Create followed by Overwrite raises CreateScheduled"""
    p1 = self.batch.create('d', {'a':1})
    self.assertRaises(CreateScheduled, self.batch.overwrite, 'd', {'a':2})
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=0, write=1)

    p2 = self.batch.get('d', cached=False)['d']
    self.assertStats(read=0, write=1)

    self.assertEqual(self.cleanDoc(p2.value()['doc']), {'a':1})
    self.assertStats(read=1, write=1)

  def testCreateUpdate(self):
    """Create followed by Update modified created document"""
    p1 = self.batch.create('d', {'a':1})
    p2 = self.batch.update('d', self.upd({'b':2}))
    self.assertStats(read=0, write=0)

    v1 = p1.value()
    v2 = p2.value()
    self.assertEqual(v1, v2)
    self.assertStats(read=0, write=1)

    p3 = self.batch.get('d', cached=False)['d']
    self.assertStats(read=0, write=1)

    self.assertEqual(self.cleanDoc(p3.value()['doc']), {'a':1,'b':2})
    self.assertStats(read=1, write=1)

  def testCreateDelete(self):
    """Create followed by Delete raises CreateScheduled"""
    p1 = self.batch.create('d', {'a':1})
    self.assertRaises(CreateScheduled, self.batch.delete, 'd')
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=0, write=1)

    p2 = self.batch.get('d', cached=False)['d']
    self.assertStats(read=0, write=1)

    self.assertEqual(self.cleanDoc(p2.value()['doc']), {'a':1})
    self.assertStats(read=1, write=1)

  def testOverwriteCreate(self):
    """Overwrite followed by Create raises OverwriteScheduled"""
    p1 = self.batch.overwrite('a', {'moo':'cow'})
    self.assertRaises(OverwriteScheduled,
        self.batch.create, 'a', {'cow':'moo'})
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=1, write=2)

    p2 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=2)

    self.assertEqual(self.cleanDoc(p2.value()['doc']), {'moo':'cow'})
    self.assertStats(read=2, write=2)

  def testOverwriteOverwrite(self):
    """Overwrite followed by Overwrite smashes existing overwrite"""
    p1 = self.batch.overwrite('a', {'moo':'cow'})
    p2 = self.batch.overwrite('a', {'cow':'furry'})
    self.assertStats(read=0, write=0)

    v1 = p1.value()
    self.assertStats(read=1, write=2)
    v2 = p2.value()
    self.assertStats(read=1, write=2)
    self.assertEqual(v1, v2)

    p3 = self.batch.get('a')['a']
    self.assertStats(read=1, write=2)
    self.assertEqual(self.cleanDoc(p3.value()['doc']), {'cow':'furry'})
    self.assertStats(read=1, write=2)

    p4 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=2)
    self.assertEqual(p3.value(), p4.value())
    self.assertStats(read=2, write=2)

  def testOverwriteUpdate(self):
    """Overwrite followed by Update updates overwrite doc"""
    p1 = self.batch.overwrite('a', {'moo':'cow'})
    p2 = self.batch.update('a', self.upd({'cow':'furry'}))
    self.assertStats(read=0, write=0)

    v1 = p1.value()
    self.assertStats(read=1, write=2)
    v2 = p2.value()
    self.assertStats(read=1, write=2)
    self.assertEqual(v1, v2)

    p3 = self.batch.get('a')['a']
    self.assertStats(read=1, write=2)
    self.assertEqual(self.cleanDoc(p3.value()['doc']),
        {'moo':'cow', 'cow':'furry'})
    self.assertStats(read=1, write=2)

    p4 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=2)
    self.assertEqual(p3.value(), p4.value())
    self.assertStats(read=2, write=2)

  def testOverwriteDelete(self):
    """Overwrite followed by Delete becomes a delete"""
    p1 = self.batch.overwrite('a', {'moo':'cow'})
    p2 = self.batch.delete('a')
    self.assertStats(read=0, write=0)

    v1 = p1.value()
    self.assertStats(read=1, write=1)
    v2 = p2.value()
    self.assertStats(read=1, write=1)
    self.assertEqual(v1, v2)

    p3 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertRaises(ResourceNotFound, p3.value)
    self.assertStats(read=2, write=1)

  def testUpdateCreate(self):
    """Update followed by Create raises UpdateScheduled"""
    p1 = self.batch.update('a', self.upd({'moo':'cow'}))
    self.assertRaises(UpdateScheduled, self.batch.create, 'a', {'b':'c'})
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=1, write=1)

    expected = dict(self.a)
    expected['moo']='cow'
    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(self.cleanDoc(p2.value()['doc']), expected)
    self.assertStats(read=1, write=1)

    p3 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(p2.value(), p3.value())
    self.assertStats(read=2, write=1)

  def testUpdateOverwrite(self):
    """Update followed by Overwrite raises UpdateScheduled"""
    p1 = self.batch.update('a', self.upd({'moo':'cow'}))
    self.assertRaises(UpdateScheduled, self.batch.overwrite, 'a', {'b':'c'})
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=1, write=1)

    expected = dict(self.a)
    expected['moo']='cow'
    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(self.cleanDoc(p2.value()['doc']), expected)
    self.assertStats(read=1, write=1)

    p3 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(p2.value(), p3.value())
    self.assertStats(read=2, write=1)

  def testUpdateUpdate(self):
    """Update followed by Update creates composite update"""
    p1 = self.batch.update('a', self.upd({'moo':'cow'}))
    p2 = self.batch.update('a', self.upd({'cow':'furry'}))
    self.assertStats(read=0, write=0)

    self.assertEqual(p1.value(), p2.value())
    self.assertStats(read=1, write=1)

    expected = dict(self.a)
    expected['moo']='cow'
    expected['cow']='furry'
    p3 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(self.cleanDoc(p3.value()['doc']), expected)
    self.assertStats(read=1, write=1)

    p4 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(p3.value(), p4.value())
    self.assertStats(read=2, write=1)

  def testUpdateDelete(self):
    """Update followed by Delete raises UpdateScheduled"""
    p1 = self.batch.update('a', self.upd({'moo':'cow'}))
    self.assertRaises(UpdateScheduled, self.batch.delete, 'a')
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=1, write=1)

    expected = dict(self.a)
    expected['moo']='cow'
    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(self.cleanDoc(p2.value()['doc']), expected)
    self.assertStats(read=1, write=1)

    p3 = self.batch.get('a', cached=False)['a']
    self.assertStats(read=1, write=1)
    self.assertEqual(p2.value(), p3.value())
    self.assertStats(read=2, write=1)

  def testDeleteCreate(self):
    """Delete followed by Create raises DeleteScheduled"""
    p1 = self.batch.delete('a')
    self.assertRaises(DeleteScheduled, self.batch.create, 'a', {'moo':'cow'})
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=1, write=1)

    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertRaises(ResourceNotFound, p2.value)
    self.assertStats(read=2, write=1)

  def testDeleteOverwrite(self):
    """Delete followed by Overwrite raises DeleteScheduled"""
    p1 = self.batch.delete('a')
    self.assertRaises(DeleteScheduled, self.batch.overwrite, 'a', {'moo':'cow'})
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=1, write=1)

    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertRaises(ResourceNotFound, p2.value)
    self.assertStats(read=2, write=1)

  def testDeleteUpdate(self):
    """Delete followed by Update raises DeleteScheduled"""
    p1 = self.batch.delete('a')
    self.assertRaises(DeleteScheduled, self.batch.update, 'a',
        self.upd({'moo':'cow'}))
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=1, write=1)

    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertRaises(ResourceNotFound, p2.value)
    self.assertStats(read=2, write=1)

  def testDeleteDelete(self):
    """Delete followed by Delete raises DeleteScheduled"""
    p1 = self.batch.delete('a')
    self.assertRaises(DeleteScheduled, self.batch.delete, 'a')
    self.assertStats(read=0, write=0)

    p1.value()
    self.assertStats(read=1, write=1)

    p2 = self.batch.get('a')['a']
    self.assertStats(read=1, write=1)
    self.assertRaises(ResourceNotFound, p2.value)
    self.assertStats(read=2, write=1)

class TestCouchKit(BaseTest):
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
    return

  def testDelete(self):
    """Document deletion works"""
    result = self.ck.bulk_save([{'_id':'a','_rev':self.a_rev,'_deleted':True}])
    map(self.normalize_revision, result)
    self.assertEqual(result, [{'rev':2,'id':'a'}])

    result = self.ck.all_docs(keys=['a'], include_docs=True)
    self.assertEqual(1, len(result))

    for r in result:
      self.assertEqual(None, r.get('doc'))

class FakeResults(object):
  def __init__(self, results):
    self.__results = results

  def all(self):
    return copy.deepcopy(self.__results)

  def __iter__(self):
    for res in self.__results:
      yield copy.deepcopy(res)

  def __len__(self):
    return len(self.__results)

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

