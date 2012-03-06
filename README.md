# Sedan - A Lazy wrapper for CouchDbKit

## Installation

### From Github

    $ git clone https://github.com/tsuraan/sedan
    $ cd sedan
    $ python setup.py install

## Usage

The basic idea of this library is to postpone database operations until the
results are needed.  Instead of returning actual database values, methods
return Promise objects.  No actual database interaction is done until a
Promise's ```.value()``` method is called; this method will either return a
value or raise an appropriate CouchDbKit exception.

All the database interaction is done through the CouchBatch class.  It is a
wrapper around CouchDbKit's Database, so we start like this:

```python
>>> import couchdbkit
>>> import sedan
>>> server   = couchdbkit.Server()
>>> database = server['testdb']
>>> batch    = sedan.CouchBatch(database)
```

### Reading Data

Reading data is done through CouchBatch's ```.get()``` method.  Get takes any
number of keys, and returns a dictionary mapping those keys into promises.
So, we can "read" some data like this:

```python
>>> initial = batch.get('first', 'second')
>>> more    = batch.get('third', 'fourth')
>>> first = initial['first'].value()
>>> third = more['third'].value()
```

The actual database request is delayed until a ```.value()``` is called, and
once it is, all the pending promises are fulfilled, so only a single database
request is done.

### Writing data

We have four data writing methods: create, overwrite, update, and delete.
Create takes a key and a value, and returns a Promise that creates a new
document in the database or raises ResourceConflict.  Overwrite is the same a
create but it won't raise ResourceConflict; it will just stomp over whatever
exists.  Update takes a function that transforms the currently-stored
dictionary into a new one, which will then be stored.  Delete deletes
something from the database or raises ResourceNotFound.

#### Creating Documents

A simple create looks like this:

```python
>>> promise = batch.create('key', {'document' : 'stuff'})
>>> promise.value()
```

That ```promise.value()``` will either return couch's friendly creation
response (a dictionary with your id and the doc's revision) or it will raise
ResourceConflict if there is already something stored at that key.

#### Overwriting Documents

Overwrite looks like this:

```python
>>> promise = batch.create('key', {'document': 'overwrite'})
>>> promise.value()
```

The ```promise.value()``` here will almost always give a successful response
(dictionary with your id and the doc's new revision); it may fail when the key
is under serious write contention, but that really shouldn't happen often.

#### Updating Documents

Updating a document is a safe alternative to reading and then overwriting a
document, and it's more convenient than using couchdbkit directly to read a
document and its revision, update the document as desired, then trying to
write the document with the revision it had, and catching resource conflicts
and trying again.  That's a pain, and that's what this library does for you.
Instead of playing that game, you can just provide a function that takes the
current value of the stored document and returns a new value for it (or
```None``` to skip the update entirely).  An example of this is:

```python
>>> def update1(doc):
>>>   counter = doc.get('counter', 0)
>>>   doc['counter'] = counter+1
>>>   return doc
>>> promise = batch.update('key', update1)
>>> promise.value()
```

As with overwrite, the ```promise.value()``` is very unlikely to fail due to
conflicts; the commit function retries numerous times in the case of
conflicts, so it will probably succeed eventually.  If there is nothing stored
at the desired key, ```promise.value()``` will raise a ResourceNotFound.

#### Deleting documents

Deleting a document removes it.  That's pretty simple...

```python
>>> promise = batch.delete('key')
>>> promise.value()
```

Delete is very unlikely to fail with a ResourceConflict, but if the key is not
present, it will raise a ResourceNotFound.

