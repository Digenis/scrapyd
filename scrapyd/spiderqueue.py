import json
import sqlite3

from zope.interface import implementer

from scrapyd.interfaces import ISpiderQueue
from scrapyd.sqlite import JsonSqlitePriorityQueue


@implementer(ISpiderQueue)
class SqliteSpiderQueue(object):

    def __init__(self, database=None, table='spider_queue'):
        self.database = database or ':memory:'
        assert not table.startswith("sqlite_"), 'Invalid table name %r' % table
        self.table, self.esc_table = table, '"%s"' % table.replace('"', '""')
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY, " \
            "priority REAL KEY, message BLOB)" % self.esc_table
        self.conn.execute(q)

    def encode(self, obj):
        return sqlite3.Binary(json.dumps(obj).encode('ascii'))

    def decode(self, text):
        return json.loads(bytes(text).decode('ascii'))

    def add(self, name, **spider_args):
        message = spider_args.copy()
        message['name'] = name
        priority = float(spider_args.pop('priority', 0))
        q = "INSERT INTO %s (priority, message) VALUES (?,?)" % self.esc_table
        self.conn.execute(q, (priority, self.encode(message)))
        self.conn.commit()

    def pop(self):
        q = "SELECT id, message FROM %s ORDER BY priority DESC LIMIT 1" \
            % self.esc_table
        idmsg = self.conn.execute(q).fetchone()
        if idmsg is None:
            return
        id, msg = idmsg
        q = "DELETE FROM %s WHERE id=?" % self.esc_table
        c = self.conn.execute(q, (id,))
        if not c.rowcount: # record vanished, so let's try again
            self.conn.rollback()
            return self.pop()
        self.conn.commit()
        return self.decode(msg)

    def count(self):
        q = "SELECT count(*) FROM %s" % self.esc_table
        return self.conn.execute(q).fetchone()[0]

    def list(self):
        q = "SELECT message FROM %s ORDER BY priority DESC" % self.esc_table
        return [self.decode(m) for m, in self.conn.execute(q)]

    def remove(self, func):
        q = "SELECT id, message FROM %s" % self.esc_table
        n = 0
        for id, msg in self.conn.execute(q):
            if func(self.decode(msg)):
                q = "DELETE FROM %s WHERE id=?" % self.esc_table
                c = self.conn.execute(q, (id,))
                if not c.rowcount: # record vanished, so let's try again
                    self.conn.rollback()
                    return self.remove(func)
                n += 1
        self.conn.commit()
        return n

    def clear(self):
        self.conn.execute("DELETE FROM %s" % self.esc_table)
        self.conn.commit()
