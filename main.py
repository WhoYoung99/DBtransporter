#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-

import os
import sys
import sqlite3 as lite
import subprocess
from check_environment import *
from func_tools import ind_finder

FILE_IN = 'db_dump.dat'
FILE_OUT = 'XXXX.dat'


class DatabaseManager(object):
    def __init__(self, db=':memory:'):
        self.conn = lite.connect(db)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.cur = self.conn.cursor()

    def dumping_tb(self, schema, data):
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


def cleanup(f_name, folder=''):
    '''
    f_name: blabla_blablabla_..._schema/data
    '''
    tb_name = '_'.join(f_name.split('_')[:-1]) # delete _schema & _data
    f_type = f_name.replace('_', '.').split('.')[-1]
    # print("Type: {0}, Table Name: {1}\n".format(f_type, tb_name))
    with open('./{0}/{1}'.format(folder, f_name)) as file_:
        raw = file_.readlines()

    if f_type == 'schema':
        # Locate the CREATE TABLE
        aim = "CREATE TABLE {0} (\n".format(tb_name)
        ind_start = raw.index(aim)
        ind_end = raw.index(");\n")
        # Retrieve only CREATE TABLE part
        clean_data = raw[ind_start:ind_end+1]
        # Remove CONSTRAINT
        if clean_data[-2].startswith('    CONSTRAINT '):
            del clean_data[-2]
            clean_data[-2] = ''.join(clean_data[-2].split(',')) # Delete the comma
        # Remove brackets
        clean_data = [l.replace('[]', '') for l in clean_data]
        return clean_data

    elif f_type == 'data':
        # ind_start = ['COPY {0} ('.format(tb_name) in l for l in raw].index(True)
        ind_start = ind_finder('COPY {0} ('.format(tb_name), raw)+1 # Begin from next line
        ind_end = ind_finder('\.', raw, reverse=True)
        clean_data = raw[ind_start:-(ind_end+1)] # [ 'string', 'string', ... ]
        return clean_data
    else:
        print("Cannot detect file type, do it on your own...")
        return None


def writing_file(content, fout_name):
    with open(fout_name, 'w') as file_:
        file_.writelines(['{0}'.format(line) for line in content])
    print("Export to: %s" % os.path.join(os.getcwd(), fout_name))


def pg_restore(tb_name, f_type, dump_name='db_dump.dat.decrypted', folder=''):
    '''
    tb_name: no _schema or _data
    output: tb_name + f_type
    '''
    if os.path.isfile(dump_name):
        switch_type = {'data':'-a', 'schema':'-s'}   
        call = "pg_restore -t {0} {1} -f ./{4}/{0}_{2} {3}".\
                format(
                        tb_name,
                        switch_type[f_type],
                        f_type,
                        dump_name,
                        folder
                        )
        ret = exec_cmd(call, debug=True)
        return ret
    else:
        return 1


def main():
    #
    ### DECRYPTING ###
    ### default output name: db_dump.dat.decrypted
    #
    do_decrypt = decryptingDump(FILE_IN, FILE_OUT, decrypt_db_tool='decrypt_db_tool.py')

    #
    ### CHECK REQUIRED TOOLS ###
    ### postgresql-client-9.5
    ### SQLite3
    # 
    do_install = checkingTools()

    #   
    ### TARGET TABLES ###
    #
    CONFIG_TABLE = "ConfigTable.dat"
    if not os.path.isfile(CONFIG_TABLE):
        sys.exit("[ERROR] Required config missing, program abort...")
    else:
        with open(CONFIG_TABLE, 'r') as file_:
            tables = file_.read().split()
            if tables == []:
                sys.exit('ConfigTable.dat is empty, program abort....')
            else:
                print("[Pass] Load ConfigTable.dat...")

    #
    ###
    #
    if leaf_table():
        sys.exit('[ERROR] Something goes wrong while creating leaf tables...')

    #
    ### EXPORT TABLES INFO ###
    #
    db = DatabaseManager() # Create DB instance
    for table in tables:
        ## Schema (if work) -> then Data
        table_name = '{0}_schema'.format(table)
        pg_restore(table, f_type='schema')
        root_name = root_table(table_name)
        print(table_name)

        if pg_restore(root_name, f_type='schema') == 0:
            schema_cleaned = cleanup('{0}_schema'.format(root_name)) # for latter executescript
            # print(schema_cleaned)
            print(''.join(cleanup('{0}_schema'.format(root_name)))) # Print full create table script

            table_name = '{0}_data'.format(table) # table_name for Data
            if table == 'tb_sandbox_parent_result':
                if pg_restore(table, f_type='data') == 0:
                    data_cleaned = cleanup(table_name)
                    data_cleaned = [tuple(i.split('\t')) for i in data_cleaned]
                    # print(data_cleaned[:2])

                else:
                    sys.exit("[ERROR] pg_restore on {0} failed, program abort...".format(table_name))
            else: # Required subtable content
                ## Parsing subtables
                data_cleaned = []
                for sub in os.listdir(os.path.join(os.getcwd(), table)):
                    print("table name: %s" % sub)
                    sub_data = cleanup(sub, folder=table)
                    sub_data = [tuple(i.split('\t')) for i in sub_data]
                    data_cleaned += sub_data

        else:
            sys.exit("[ERROR] pg_restore on {0} failed, program abort...".format(table_name))
        
        # print(data_cleaned)

        test = DatabaseManager()
        test.dumping_tb(schema_cleaned, data_cleaned)

        tb_name = schema_cleaned[0][13:-3]
        #####
        query = "SELECT * FROM {0} LIMIT 2".format(tb_name)
        #####
        print(query)
        print(test.fetching(query))


if __name__ == '__main__':
    main()