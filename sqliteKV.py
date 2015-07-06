__all__ = ['DB']
import sqlite3

try:
    import cPickle as pickle
except ImportError:
    import pickle

_dumps = lambda o: pickle.dumps(o)
_loads = lambda d: pickle.loads(str(d))

class _Table(object):
    _DEFAULT_TABLE_ = '__KVS_TABLE__'

    def __init__(self, DB, name=None):
        self._db = DB._db
        self._cursor = DB._cursor
        self._table_(name)

    def _table_(self, table):
        table = table or self._DEFAULT_TABLE_
        _new_table = "CREATE TABLE IF NOT EXISTS  %s ( k PRIMARY KEY,v)" % table
        self._statement_insert = "insert into %s (k,v) values(:1,:2)" % table
        self._statement_delete = "delete from %s where k=:1" % table
        self._statement_update = "update %s set v=:1 where k=:2" % table
        self._statement_put = "insert or replace into %s(k,v) values(:1,:2)" % table
        self._statement_get = "select v from %s where k=:1" % table
        self._statement_has_key = "select count(*) from %s where k=:1" % table
        self._statement_keys = "select k from %s " % table
        self._statement_items = "select k,v from %s " % table
        self._statement_count = "select count(*) from %s " % table
        self._table = table
        self._db.execute(_new_table)

    def keys(self):
        _keys = self._db.execute(self._statement_keys).fetchall()
        return [k for k, in _keys]

    def iteritems(self):
        return self._db.execute(self._statement_items)

    def items(self):
        return self.iteritems().fetchall()

    def count(self):
        return self._db.execute(self._statement_count).fetchone()[0]

    def _query(self, key):
        key = _dumps(key)
        data = self._cursor.execute(self._statement_get, (key,)).fetchone()
        if not data: return None
        return data[0]

    def _insert(self, key, value):
        key = _dumps(key)
        value = _dumps(value)
        self._cursor.execute(self._statement_insert, (key, value))

    def _update(self, key, value):
        key = _dumps(key)
        value = _dumps(value)
        self._cursor.execute(self._statement_update, (value, key))

    def _delete(self, key):
        key = _dumps(key)
        self._cursor.execute(self._statement_delete, (key,))

    def _put(self, key, value):
        key = _dumps(key)
        value = _dumps(value)
        self._cursor.execute(self._statement_put, (key, value))

    def has_key(self, key):
        key = _dumps(key)
        r = self._cursor.execute(self._statement_has_key, (key,)).fetchone()[0]
        return True if r else False

    def get(self, key):
        return self._query(key)

    def put(self, key, value):
        try:
            self._put(key, value)
            self._db.commit()
        except Exception as e:
            self._db.rollback()
            raise e

    def pop(self, key):
        if self.has_key(key):
            try:
                value = self._query(key)
                self._delete(key)
                self._db.commit()
                return value
            except Exception as e:
                self._db.rollback()
                raise e

    def remove(self, key):
        if self.has_key(key):
            try:
                self._delete(key)
                self._db.commit()
            except Exception as e:
                self._db.rollback()
                raise e

    def putMany(self, rows):
        try:
            self._cursor.executemany(self._statement_put, [(_dumps(k), _dumps(v)) for k, v in rows])
            self._db.commit()
        except sqlite3.DatabaseError:
            self._db.rollback()
            for key, value in rows:
                try:
                    self._put(key, value)
                except Exception as e:
                    self._db.rollback()
                    raise e
            self._db.commit()
        except Exception as e:
            self._db.rollback()
            raise e

class DB(object):
    def __init__(self, filename):
        conn = sqlite3.connect(filename)
        conn.row_factory = DB._row_factory
        cursor = conn.cursor()
        self._db = conn
        self._cursor = cursor

    @staticmethod
    def _row_factory(cursor, row):
        result = []
        for idx, col in enumerate(cursor.description):
            if col[0].lower() in ('k', 'v'):
                result.append(_loads(row[idx]))
            else:
                result.append(row[idx])
        return result

    def close(self):
        self._db.rollback()
        self._cursor.close()
        self._db.close()

    def table(self, name=None):
        return _Table(self, name)
