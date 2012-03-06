"""This is where our couchdbkit view results emulator lives.  It's not really
the same as the couchdbkit view, but it does support len and iteration, so
it's a fair-enough drop-in.
"""

class ViewResults(object):
  def __init__(self, towrap, cache):
    self.__rows  = towrap
    self.__cache = cache

  def __len__(self):
    return len(self.__rows)

  def __iter__(self):
    for row in self.__rows:
      if 'doc' not in row:
        yield row
        continue

      update = False
      key    = row['id']
      if key in self.__cache:
        cver = self.__cache[key]['value']['rev']
        rver = row['doc']['_rev']
        if rver > cver:
          update = True
      else:
        update = True

      if update:
        self.__cache[key] = {
            'doc'   : row['doc'],
            'id'    : key,
            'key'   : key,
            'value' : {'rev':row['doc']['_rev']},
            }
      yield row

