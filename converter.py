#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
import re
import os
import sys
import sqlite3 as lite
import subprocess

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


def exec_cmd(cmd, debug=False):
    if debug: 
        ret = subprocess.call(cmd, shell=True)
    else:
        ret = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
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

def root_table(table_name):
    '''
    Find the root table name, if none, return itself
    LIMITATION: ONLY search 1 upper inherit table
    ex. if tb_child ---inherit--> tb_parent ---inherit--> tb_result
    then root_table(tb_child) only return tb_parent instead of tb_result

    '''
    with open(table_name) as file_:
        raw = file_.readlines()
    
    ind_root = ind_finder('INHERITS (', raw, reverse=True)
    if ind_root == None:
        return table_name
    else:
        root = raw[-(ind_root+1)][10:-3]
        return root
        # return "{0}_schema".format(root)

def leaf_table():
    '''
    Find all leaves of table
    '''
    table_start_with = ['TABLE DATA public tb_cav_logs_', 'TABLE DATA public tb_tmufe_logs_']
    command = "pg_restore -l -f TableAll.dat db_dump.dat.decrypted"
    ret = exec_cmd(command)
    if ret:
        print('[ERROR] Cannot create subtable lists...')
        return ret
    else:
        with open('TableAll.dat', 'r') as file_:
            raw = file_.readlines()

        leaves = [i.split(' ')[-2] for i in raw for target in table_start_with if (target in i)]
        i = len(leaves)
        assert i/2 == int(i/2)
        folder = 'tb_cav_logs'
        if os.path.isdir('./{0}'.format(folder)):
            print('[Warning] Folder already exist, start dumping leaves...')
            pass
        else:
            print('[Warning] Create folder, start dumping leaves...')
            exec_cmd('mkdir {0}'.format(folder))
        for leaf in leaves[:int(i/2)]:
            ret = pg_restore(leaf, 'data', folder=folder)
            if ret:
                print('[ERROR] Failed to pg_restore {0}'.format(leaf))
                return ret
        folder = 'tb_tmufe_logs'
        if os.path.isdir('./{0}'.format(folder)):
            print('[Warning] Folder already exist, start dumping leaves...')
            pass
        else:
            print('[Warning] Create folder, start dumping leaves...')
            exec_cmd('mkdir {0}'.format(folder))
        for leaf in leaves[int(i/2):]:
            ret = pg_restore(leaf, 'data', folder=folder)
            if ret:
                print('[ERROR] Failed to pg_restore {0}'.format(leaf))
                return ret
        ## Logging
        write_leaf = '\n'.join(leaves)
        with open('TableList.dat', 'w') as file_:
            file_.write(write_leaf)

        return 0


def cleanup(f_name):
    '''
    f_name: blabla_blablabla_..._schema/data
    '''
    tb_name = '_'.join(f_name.split('_')[:-1]) # delete _schema & _data
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
            clean_data[-2] = ''.join(clean_data[-2].split(',')) # Delete the comma
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


# def export_db(tb_name, dump_name='db_dump.dat.decrypted'):
#     if os.path.isfile(dump_name):
#         # call_schema = "pg_restore -t {0} -s -f {0}_schema {1}".format(tb_name, dump_name)
#         # call_schema = "pg_restore -t tb_sandbox_result -s -f tb_sandbox_result_schema {1}".format(tb_name, dump_name)
#         call_data = "pg_restore -t {0} -a -f {0}_data {1}".format(tb_name, dump_name)
#         ret_schm = exec_cmd(call_schema)
#         ret_data = exec_cmd(call_data)
#         return ret_data + ret_schm
#     else:
#         return 1


def main():
    #
    ### DECRYPTING ###
    ### default output name: db_dump.dat.decrypted
    #
    if os.path.isfile("db_dump.dat.decrypted"):
        print("[Pass] Decrypted dump file already exist...")
    else:
        command = "python decrypt_db_tool.py"
        ret = exec_cmd(command, debug=True)
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
            if pg_restore(table, f_type='data') == 0:
                data_cleaned = cleanup(table_name)
                data_cleaned = [tuple(i.split('\t')) for i in data_cleaned]

            else:
                sys.exit("[ERROR] pg_restore on {0} failed, program abort...".format(table_name))
        else:
            sys.exit("[ERROR] pg_restore on {0} failed, program abort...".format(table_name))

        test = DatabaseManager()
        test.dumping_tb(schema_cleaned, data_cleaned)
        
        tb_name = schema_cleaned[0][13:-3]
        #####
        query = "SELECT * FROM {0} LIMIT 2".format(tb_name)
        #####
        print(query)
        print(test.fetching(query))
        # if export_db(table) == 0: # No error
        #     schema_cleaned = cleanup('{0}_schema'.format(table))
        #     # schema_cleaned = ''.join(cleanup('tb_sandbox_result_schema'))
        #     print(schema_cleaned)
        #     # exporting = writing_file(schema_cleaned, '{0}_Cschema'.format(table))
        #     data_cleaned = cleanup('{0}_data'.format(table))
        #     data_cleaned = [tuple(i.split('\t')) for i in data_cleaned]
        #     # exporting = writing_file(data_cleaned, '{0}_Cdata'.format(table))
        #     print("[Pass] Finish pg_restore on table: {0}".format(table))
        # else:
        #     sys.exit("[ERROR] Unable to do pg_restore, program abort...")

    #
    ### INTO SQLite3 ##
    #
    # cur = create_db(schema_cleaned)
    # # Import Data
    # cur.executemany("INSERT INTO tb_sandbox_result (id, receivedtime, sha1, severity, overallseverity, report, filemd5, parentsha1, origfilename, malwaresourceip, malwaresourcehost, analyzetime, truefiletype, filesize, pcapready, dropsha1list, virusname, va_threat_category_ids, va_virusname_list) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", data_cleaned)
    # con.commit()

    # cur.execute('SELECT * FROM tb_sandbox_result')
    # print(len(cur.fetchall()))
    # con.close()


if __name__ == '__main__':
    main()