# Class
import sqlite3 as lite

class DatabaseManager(object):
    def __init__(self, db='TDADB.db'):
        self.conn = lite.connect(db)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.cur = self.conn.cursor()

    def restore(self, schema, data):
        # print(''.join(schema))
        t = ''.join(schema)
        self.cur.executescript(t)
        tb_name = schema[0][13:-3]
        n = len(schema)-2 # Excluding first line 'CREATE TABLE ... (' and last line ');'
        items = schema[1:-1]
        # print(items)
        header = [i.split(' ')[4] for i in items]
        # print(','.join(header))
        add_entry = "INSERT INTO {0} ({1}) VALUES ({2})".format(tb_name, ','.join(header), ','.join('?'*n))
        # print(add_entry)
        self.cur.executemany(add_entry, data)
        self.conn.commit()

    def fetching(self, arg):
        self.cur.execute(arg)
        return self.cur.fetchall()

    def __del__(self):
        self.conn.close()