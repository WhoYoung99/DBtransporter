#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-

import os
import re
import sys
import sqlite3 as lite
import subprocess
import pprint as pp
from copy import deepcopy
from check_environment import decryptingDump, checkingTools
from data_dump import ind_finder, leaf_table, readingConfigTable
from data_extract import pg_restore, root_table, cleanup, dumpFileExtract
from cmd_tool import exec_cmd
from parse_xml import parserXML
import xml.etree.ElementTree as ET


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


def writing_file(content, fout_name):
    with open(fout_name, 'w') as file_:
        file_.writelines(['{0}'.format(line) for line in content])
    print("Export to: %s" % os.path.join(os.getcwd(), fout_name))


FILE_IN = 'db_dump.dat'
FILE_OUT = 'XXXX.dat'
ITEM_LIST = [
        'ParentSHA1',
        'FileSHA1',
        'FileMD5',
        'TrueFileType',
        'FileSize',
        'OrigFileName',
        'GRIDIsKnownGood',
        'AnalyzeTime',
        'VirusName',
        'AnalyzeStartTime',
        'ParentChildRelationship'
        ]


def main():
    #
    ###### Part I: chekc_environment.py ######
    #
    do_decrypt = decryptingDump(FILE_IN, FILE_OUT, decrypt_db_tool='decrypt_db_tool.py')
    assert do_decrypt == 0

    do_install = checkingTools()
    assert do_install == 0

    #
    ###### Part II: data_dump.py ######
    #
    tables = readingConfigTable(file_name='ConfigTable.dat')

    find_subtables = leaf_table(FILE_OUT)
    assert find_subtables == 0

    #
    ###### Part III: data_extract.py ######
    #
    test = DatabaseManager()

    with open('table_va_sample_results.txt', 'r') as f:
        va_results_schema = f.readlines()
    # print(va_results_schema)

    for table in tables:
        print('[Process] Reading table data: {0}'.format(table))
        schema, data = dumpFileExtract(table, db_dump=FILE_OUT)
        print('[Process] Create table in SQLite: {0}'.format(table))
        test.dumping_tb(schema, data)
        
        #####
        actual_table = schema[0][13:-3]
        # query = "SELECT report FROM {0}".format(actual_table)
        #####
        # print(query)
        # raw = test.fetching(query)
        # data = [i[0].replace('\\n', '') for i in raw]
        # parse = [parserXML(i, ITEM_LIST, True) for i in data]
        # va_results_data = [j for i in parse for j in i]
        
        # test.dumping_tb(va_results_schema, va_results_data)


        # query = "SELECT * FROM {0} LIMIT 1".format(actual_table)
        # pp.pprint(test.fetching(query))



        # query = "SELECT * FROM {0}".format(tb_name)
        # raw = test.fetching(query)

        # target_col = [i[2].split(',') for i in raw]
        # DNS = [i[0] for i in target_col]
        # HTTP = [i[1] for i in target_col]
        # Email = [i[2] for i in target_col]
        # sum_DNS = sum([int(re.search(r'\d+', i).group()) for i in DNS])
        # sum_HTTP = sum([int(re.search(r'\d+', i).group()) for i in HTTP])
        # sum_Email = sum([int(re.search(r'\d+', i).group()) for i in Email])
        # pp.pprint([sum_DNS, sum_HTTP, sum_Email])
        

if __name__ == '__main__':
    main()