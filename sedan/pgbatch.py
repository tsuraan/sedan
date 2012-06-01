from couchdbkit.exceptions import ResourceConflict
from couchdbkit.exceptions import ResourceNotFound
from .promise import Promise
from .result import DbFailure
from .result import DbValue
import psycopg2
import json

class PgBatch(object):
  """A batch-like class that uses postgres instead of couch.  This is
  implementing only the methods that I need it to; if it's good, maybe it will
  get more methods.
  """
  def __init__(self, psyco):
    self.db     = psyco
    self.writes = []

  def get(self, *keys, **kwargs):
    """Return a dictionary with a promise stored for each key given.  The
    promise will either yield the database's record for the key or raise a
    ResourceNotFound.

    If a record is yielded, it will be a dictionary with the outermost key of
    'doc', and some actual useful data stored within.  That's silly here, but
    it resembles what couch does.
    """
    cursor = self.db.cursor()
    try:
      cursor.execute("select key, obj, rev from git where key in %s",
          (tuple(keys),))
      rows = dict( (row[0], (row[1], row[2])) for row in cursor )
    finally:
      cursor.close()

    result = {}
    for key in keys:
      try:
        obj, rev    = rows[key]
        doc         = json.loads(obj)
        doc['_id']  = key
        doc['_rev'] = rev
        row         = DbValue({'doc':doc})
      except KeyError:
        row = DbFailure(ResourceNotFound({}))
      promise = Promise(key, lambda: None)
      promise._fulfill(row)
      result[key] = promise
    return result

  def create(self, key, doc):
    """Store the value in postgres if it's not already there.  If it is, the
    returned promise will raise ResourceConflict.
    """
    doc         = dict(doc)
    if '_id' in doc:
      del doc['_id']
    if '_rev' in doc:
      del doc['_rev']

    doc         = json.dumps(doc)
    promise     = Promise(key, self.do_writes)
    cursor      = self.db.cursor()
    try:
      cursor.execute("INSERT INTO Git(key, obj, rev) VALUES(%s, %s, 1)",
          (key, doc))
      self.db.commit()
      promise._fulfill(DbValue({'rev':1, 'id':key}))
    except psycopg2.Error:
      self.db.rollback()
      promise._fulfill(DbFailure(ResourceConflict({})))
    finally:
      cursor.close()
    
    self.writes.append(promise)
    return promise

  def do_writes(self):
    """return all the promises that have been enqueued since the last call to
    do_writes, and also do the writes associated with those promises.
    """
    writes      = self.writes
    self.writes = []
    return writes

  @property
  def ck(self):
    """Give a fake couchkit object that has a save_doc method.  This save_doc
    should accurately emulate the save_doc method of the couchkit library.
    """
    return DocSaver(self.db)

class DocSaver(object):
  """A really hacky stand-in for a couchdbkit Database object; this one only
  supports the save_doc method, and probably poorly.
  """
  def __init__(self, psyco):
    self.db = psyco

  def save_doc(self, doc):
    """Save a document.  This will look at the doc's _id key to determine the
    database key, and it will look at the doc's _rev key to determine what (if
    any) revision the currently stored document should have.
    """
    if '_rev' in doc:
      rev = doc['_rev']
      del doc['_rev']
    else:
      rev = None
    key = doc['_id']
    del doc['_id']
    doc = json.dumps(doc)

    cursor = self.db.cursor()
    try:
      if not rev:
        cursor.execute("INSERT INTO Git(key, obj, rev) VALUES(%s, %s, 1)",
            (key, doc))
        self.db.commit()
      else:
        cursor.execute("UPDATE Git SET obj=%s, rev=%s WHERE key=%s AND rev=%s",
            (doc, rev+1, key, rev))
        if cursor.rowcount:
          self.db.commit()
        else:
          raise psycopg2.Error
    except psycopg2.Error:
      raise ResourceConflict({})

    finally:
      cursor.close()
