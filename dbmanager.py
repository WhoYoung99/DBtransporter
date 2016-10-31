# Class
import sqlite3 as lite

class DatabaseManager(object):
    def __init__(self, dbname):
        self.conn = lite.connect('./Output/{0}'.format(dbname))
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.mapping = dict()
        self.cur = self.conn.cursor()

    # def create(self, schema_list):
    #     for schema in schema_list:
    #         self.cur.executescript(''.join(schema))
    #         table = schema[0][13:-2]
    #         column_namelist = [i.split(' ')[4] for i in schema[1:-1]]
    #         self.mapping[table] = column_namelist
    #     self.conn.commit()

    # def restore(self, data_list):
    #     fixbug_dict = dict(zip(['tb_sandbox_parent_result', 'tb_cav_logs', 'tb_tmufe_logs', 'tb_protocol_request_logs'], 
    #                             ['tb_sandbox_result', 'tb_cav_total_logs', 'tb_tmufe_total_logs', 'tb_protocol_request_logs']))
    #     for table, data in data_list:
    #         header = self.mapping[fixbug_dict[table]]
    #         add_entry = "INSERT INTO {0} ({1}) VALUES ({2})".format(\
    #                         fixbug_dict[table], ','.join(header), ','.join('?'*len(header)))
    #         self.cur.executemany(add_entry, data)
    #     self.conn.commit()


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