#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
import re
import os
import sys
import sqlite3 as lite
import subprocess

class DBTable:
    def __init__(self, tb_name):
        self.name = tb_name

    def __repr__(self):
        return '{0.name!r}'.format(self)

    def __str__(self):
        return 'Table name: {0.name!r}'.format(self)

def exec_cmd(cmd, debug=False):
    if debug: 
        ret = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
    else:
        ret = subprocess.call(cmd, shell=True)
    return ret


def ind_finder(words, content, reverse=False):
    '''
    Find the index of specific words segment
    return None if words were not found

    Examples: content = ['a', 'aaaab', 'c']
    ind_finder('a', content) will return 0
    ind_finder('aa', content) will return 1
    ind_finder('x', content) will return None
    '''
    counter = 0
    if reverse: content = reversed(content)
    for line in content:
        if words in line:
            return counter
        else:
            counter += 1
    return None


def cleanup(f_name):
    '''
    f_name: blabla_blablabla_..._schema/data
    '''
    tb_name = '_'.join(f_name.split('_')[:-1])
    f_type = f_name.replace('_', '.').split('.')[-1]
    # print("Type: {0}, Table Name: {1}\n".format(f_type, tb_name))
    with open(f_name) as file_:
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
            clean_data[-2] = ''.join(clean_data[-2].split(','))

        # Remove brackets
        clean_data = [l.replace('[]', '') for l in clean_data]
        return clean_data

    elif f_type == 'data':
        # ind_start = ['COPY {0} ('.format(tb_name) in l for l in raw].index(True)
        ind_start = ind_finder('COPY {0} ('.format(tb_name), raw)+1 # Begin from next line
        ind_end = ind_finder('\.', raw, reverse=True)
        clean_data = raw[ind_start:-(ind_end+1)]
        return clean_data
    else:
        print("Cannot detect file type, do it on your own...")

def writing_file(content, fout_name):
    with open(fout_name, 'w') as file_:
        file_.writelines(['{0}'.format(line) for line in content])
    print("Export to: %s" % os.path.join(os.getcwd(), fout_name))



def export_db(tb_name, dump_name='db_dump.dat.decrypted'):

    if os.path.isfile(dump_name):
        # call_schema = "pg_restore -t {0} -s -f {0}_schema {1}".format(tb_name, dump_name)
        call_schema = "pg_restore -t tb_sandbox_result -s -f tb_sandbox_result_schema {1}".format(tb_name, dump_name)
        call_data = "pg_restore -t {0} -a -f {0}_data {1}".format(tb_name, dump_name)
        ret_schm = exec_cmd(call_schema)
        ret_data = exec_cmd(call_data)
        return ret_data + ret_schm
    else:
        return 1


def main():
    #
    ### DECRYPTING ###
    ### default output name: db_dump.dat.decrypted
    #
    if not os.path.isfile("db_dump.dat.decrypted"):
        print("[Pass] Decrypted dump file already exist...")
    else:
        command = "python decrypt_db_tool.py"
        ret = exec_cmd(command)
        if ret == 0:
            print("[Pass] Finish db_dump decryption...")
        else:
            sys.exit("[ERROR] Unable to decrypt db_dump, program abort...")

    #
    ### CHECK REQUIRED TOOLS ###
    ### postgresql-client-9.5
    ### SQLite3
    # 
    command_postgre = "which pg_restore"
    ret = exec_cmd(command_postgre)
    if ret:
        print("Installing postgresql client package...") 
        exec_cmd("sudo apt-get install postgresql-client-9.5", debug=True)

    command_sqlite = "which sqlite3"
    ret = exec_cmd(command_sqlite)
    if ret:
        print("Installing SQLite3 package...")
        exec_cmd("sudo apt-get install sqlite3", debug=True)

    if exec_cmd(command_sqlite) or exec_cmd(command_postgre):
        print("Please install required tools before running the converter again.")
        sys.exit("Bye...")
    else:
        print("[Pass] Required tool checking...")

    #   
    ### TARGET TABLES ###
    #
    CONFIG_TABLE = "ConfigTable.dat"
    if not os.path.isfile(CONFIG_TABLE):
        sys.exit("[ERROR] Required config missing, program abort...")
    else:
        with open(CONFIG_TABLE, 'r') as file_:
            tables = file_.read().split()
        print("[Pass] Load ConfigTable.dat...")

    #
    ### EXPORT TABLES INFO ###
    #
    for table in tables:
        if export_db(table) == 0: # No error
            # schema_cleaned = cleanup('{0}_schema'.format(table))
            schema_cleaned = ''.join(cleanup('tb_sandbox_result_schema'))
            print(schema_cleaned)
            # exporting = writing_file(schema_cleaned, '{0}_Cschema'.format(table))
            data_cleaned = cleanup('{0}_data'.format(table))
            data_cleaned = [tuple(i.split('\t')) for i in data_cleaned]
            # exporting = writing_file(data_cleaned, '{0}_Cdata'.format(table))
            print("[Pass] Finish pg_restore on table: {0}".format(table))
        else:
            sys.exit("[ERROR] Unable to do pg_restore, program abort...")

    #
    ### INTO SQLite3 ###
    #
    con = lite.connect(':memory:')
    cur = con.cursor()
    # Create DB
    cur.executescript(schema_cleaned)
    # Import Data
    cur.executemany("INSERT INTO tb_sandbox_result (id, receivedtime, sha1, severity, overallseverity, report, filemd5, parentsha1, origfilename, malwaresourceip, malwaresourcehost, analyzetime, truefiletype, filesize, pcapready, dropsha1list, virusname, va_threat_category_ids, va_virusname_list) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", data_cleaned)
    con.commit()

    cur.execute('SELECT * FROM tb_sandbox_result')
    print(len(cur.fetchall()))
    con.close()


if __name__ == '__main__':
    main()