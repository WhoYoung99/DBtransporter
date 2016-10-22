#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-

import os
import re
import sys
import dbmanager
import sqlite3 as lite
import subprocess
import pprint as pp
from copy import deepcopy
import decryption
import extraction
import restorsion
# from check_environment import decryptingDump, checkingTools
# from data_dump import ind_finder, leaf_table, readingConfigTable
# from data_extract import pg_restore, root_table, cleanup, dumpFileExtract
from tools import *
from parse_xml import parserXML
import xml.etree.ElementTree as ET


ITEM_LIST_result = ['ParentSHA1', 'FileSHA1', 'FileMD5', 'TrueFileType',
                    'FileSize', 'OrigFileName', 'GRIDIsKnownGood', 'AnalyzeTime',
                    'VirusName', 'AnalyzeStartTime', 'ParentChildRelationship'
                    ]

ITEM_LIST_charac = ['FileSHA1', 'violatedPolicyName', 'Event',
                    'Details', 'image_type'
                    ]

DETAIL_VALUE = ['AUTH', 'CIFS', 'DHCP', 'DNS Response', 'FTP', 
                'HTTP', 'ICMP', 'IM', 'IRC', 'LDAP',
                'P2P', 'POP3', 'Remote Access', 'SMTP', 'SNMP',
                'SQL', 'TCP', 'TFTP', 'UDP', 'WEBMAIL',
                'STREAMING', 'VOIP', 'TUNNELING', 'IMAP4', 'DNS Request',
                'MAIL'
                ]

DETAIL_KEY = list(range(1, 25)) + [68, 25]
DETAIL = dict(zip(DETAIL_KEY, DETAIL_VALUE))


def main():
    print('[Initialize] Input DB: {0}; Decrypted DB: {1}'.format(DB_DUMP, DB_DUMP_DECRYPTED))

    #
    ## PartI: Decryption ##
    #
    do_decrypt = decryption.decryptingDump(DB_DUMP, DB_DUMP_DECRYPTED, decrypt_db_tool='decrypt_db_tool.py')
    assert do_decrypt == 0
    do_install = decryption.checkingTools()
    assert do_install == 0
    tables = decryption.readingConfigTable(file_name='ConfigTable.dat')
    print('ConfigTable: {0}'.format(tables))
    #
    ## PartII: Extraction ##
    #
    creat_folder = extraction.makeDirectory(tables)
    leaves = extraction.leafTable(DB_DUMP_DECRYPTED)
    for table in tables:
        do_extract = extraction.dumpFileExtract(table, DB_DUMP_DECRYPTED, leaves)
        print('[Debug] Error code - {0}'.format(do_extract))
    #
    ## PartIII: Restoration ##
    #
    db_path = './Output/{0}'.format(DB_OUT)
    if os.path.isfile(db_path):
        os.remove(db_path)
    db = dbmanager.DatabaseManager(DB_OUT)
    do_restore = restorsion.dumpFileRestore(db)
    #
    ## PartIV: Convertion ##
    #


    # test = DatabaseManager(db=DB_OUT)

    # with open('table_va_sample_results.txt', 'r') as f:
    #     va_results_schema = f.readlines()

    # with open('table_va_sample_charac.txt', 'r') as f:
    #     va_charac_schema = f.readlines()

    # with open('table_data_statistics.txt', 'r') as f:
    #     data_statistics_schema = f.readlines()

    # for table in tables:
    #     print('[Process] Reading table data: {0}'.format(table))
    #     xxxx = extraction.newFileExtract(table, DB_DUMP_DECRYPTED)
    #     break
    #     schema, data = extraction.dumpFileExtract(table, db_dump=DB_DUMP_DECRYPTED)
    #     print('[Process] Create table in SQLite: {0}'.format(table))
    #     test.restore(schema, data)
        
    #     actual_table = schema[0][13:-3]
        
    #     if table == 'tb_sandbox_parent_result':
    #         query = "SELECT report FROM {0}".format(actual_table)
    #         raw = test.fetching(query)
    #         data = [i[0].replace('\\n', '') for i in raw]
    #         parse_all = [parserXML(i, ITEM_LIST_result, True) for i in data]

    #         parse_result = [i[0] for i in parse_all]
    #         parse_charac = [i[1] for i in parse_all]
    #         va_results_data = [j for i in parse_result for j in i]
    #         va_charac_data = [j for i in parse_charac for j in i]

    #         test.restore(va_results_schema, va_results_data)
    #         test.restore(va_charac_schema, va_charac_data)

    #     elif table == 'tb_protocol_request_logs':
    #         # query = "SELECT * FROM {0} LIMIT 2".format(actual_table)
    #         # pp.pprint(test.fetching(query))
    #         query = "SELECT * FROM {0}".format(actual_table)
    #         raw = test.fetching(query)

    #         col1 = [i[0].split(',') for i in raw]
    #         protocol_anal = ','.join(list(set([DETAIL[int(j)] for i in col1 for j in i if j != '0'])))
    #         print(protocol_anal)

    #         col2 = [i[1] for i in raw]
    #         traffic = int(sum(col2)/1024**3)

    #         col3 = [i[2].split(',') for i in raw]
    #         DNS = [i[0] for i in col3]
    #         HTTP = [i[1] for i in col3]
    #         Email = [i[2] for i in col3]
    #         sum_DNS = int(sum([int(re.search(r'\d+', i).group()) for i in DNS])/1024**2)
    #         sum_HTTP = int(sum([int(re.search(r'\d+', i).group()) for i in HTTP])/1024**2)
    #         sum_Email = int(sum([int(re.search(r'\d+', i).group()) for i in Email])/1024**2)

    #         data_statistics_data = [tuple([len(protocol_anal)-1, traffic, sum_HTTP, sum_Email, sum_DNS, protocol_anal])]
    #         print(data_statistics_data[0])
    #         test.restore(data_statistics_schema, data_statistics_data)


DB_OUT = 'TDADB.db'

if __name__ == '__main__':
    if len(sys.argv) == 1:
        DB_DUMP = 'db_dump.dat'
        DB_DUMP_DECRYPTED = 'db_dump_decrypted.dat'
        main()
    elif len(sys.argv) == 3:
        DB_DUMP = sys.argv[1]
        DB_DUMP_DECRYPTED = sys.argv[2]
        main()
    else:
        error_msg = 'Expecting none or 2 system parameters, {0} recieved...'.format(len(sys.argv)-1)
        sys.exit(error_msg)