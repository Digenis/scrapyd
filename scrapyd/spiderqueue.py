import json
import sqlite3

from zope.interface import implementer

from scrapyd.interfaces import ISpiderQueue
from scrapyd.sqlite import JsonSqlitePriorityQueue


@implementer(ISpiderQueue)
class SqliteSpiderQueue(object):

    def __init__(self, database=None, table='spider_queue'):
        self.database = database or ':memory:'
        self.table = table
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = "create table if not exists %s (id integer primary key, " \
            "priority real key, message blob)" % table
        self.conn.execute(q)

    def encode(self, obj):
        return sqlite3.Binary(json.dumps(obj).encode('ascii'))

    def decode(self, text):
        return json.loads(bytes(text).decode('ascii'))

    def add(self, name, **spider_args):
        message = spider_args.copy()
        message['name'] = name
        priority = float(spider_args.pop('priority', 0))
        q = "insert into %s (priority, message) values (?,?)" % self.table
        self.conn.execute(q, (priority, self.encode(message)))
        self.conn.commit()

    def pop(self):
        q = "select id, message from %s order by priority desc limit 1" \
            % self.table
        idmsg = self.conn.execute(q).fetchone()
        if idmsg is None:
            return
        id, msg = idmsg
        q = "delete from %s where id=?" % self.table
        c = self.conn.execute(q, (id,))
        if not c.rowcount: # record vanished, so let's try again
            self.conn.rollback()
            return self.pop()
        self.conn.commit()
        return self.decode(msg)

    def count(self):
        q = "select count(*) from %s" % self.table
        return self.conn.execute(q).fetchone()[0]

    def list(self):
        q = "select message from %s order by priority desc" % self.table
        return [self.decode(m) for m, in self.conn.execute(q)]

    def remove(self, func):
        q = "select id, message from %s" % self.table
        n = 0
        for id, msg in self.conn.execute(q):
            if func(self.decode(msg)):
                q = "delete from %s where id=?" % self.table
                c = self.conn.execute(q, (id,))
                if not c.rowcount: # record vanished, so let's try again
                    self.conn.rollback()
                    return self.remove(func)
                n += 1
        self.conn.commit()
        return n

    def clear(self):
        self.conn.execute("delete from %s" % self.table)
        self.conn.commit()
