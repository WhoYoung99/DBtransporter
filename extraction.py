# DUMPING TABLES
# Input:  db_dump.dat
# Output: list of table dump files

import os
import sys
import tools


def leafTable(db_dump_decrypted):
    '''
    Find all leaves of table
    '''
    db_dump_decrypted_path = os.path.join(os.getcwd(), 'Input', db_dump_decrypted)
    table_start_with = ['TABLE DATA public tb_cav_logs_', 'TABLE DATA public tb_tmufe_logs_']
    command = "pg_restore -l -f TableAll.dat {0}".format(db_dump_decrypted_path)
    ret = tools.exec_cmd(command)
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
            print('[Warning] Folder already exist, skip dumping leaves...')
            pass
        else:
            print('[Warning] Create folder, start dumping leaves...')
            tools.exec_cmd('mkdir {0}'.format(folder))
            for leaf in leaves[:int(i/2)]:
                ret = pg_restore(leaf, 'data', db_dump_decrypted_path, folder=folder)
                if ret:
                    print('[ERROR] Failed to pg_restore {0}'.format(leaf))
                    return ret
        folder = 'tb_tmufe_logs'
        if os.path.isdir('./{0}'.format(folder)):
            print('[Warning] Folder already exist, skip dumping leaves...')
            pass
        else:
            print('[Warning] Create folder, start dumping leaves...')
            tools.exec_cmd('mkdir {0}'.format(folder))
            for leaf in leaves[int(i/2):]:
                ret = pg_restore(leaf, 'data', db_dump_decrypted_path, folder=folder)
                if ret:
                    print('[ERROR] Failed to pg_restore {0}'.format(leaf))
                    return ret
        ## Logging
        write_leaf = '\n'.join(leaves)
        with open('TableList.dat', 'w') as file_:
            file_.write(write_leaf)

        return 0

def readingConfigTable(file_name='ConfigTable.dat'):
    file_name_path = os.path.join(os.getcwd(), 'Input', file_name)
    print(file_name_path)
    if not os.path.isfile(file_name_path):
        sys.exit('[ERROR] Cannot find file {0}, program abort...'.format(file_name_path))
    else:
        with open(file_name_path, 'r') as file_:
            tables = file_.read().split()
            if tables == []:
                sys.exit('[ERROR] {0} is empty, program abort...'.format(file_name))
            else:
                print("[Pass] Load ConfigTable.dat...")
                return tables

def pg_restore(table, f_type, dump_name, folder=''):
    '''
    table: no _schema or _data
    output: table + f_type
    '''
    if os.path.isfile(dump_name):
        switch_type = {'data':'-a', 'schema':'-s'}   
        call = "pg_restore -t {0} {1} -f ./{4}/{0}_{2} {3}".\
                format(
                        table,
                        switch_type[f_type],
                        f_type,
                        dump_name,
                        folder
                        )
        print(call)
        ret = tools.exec_cmd(call, debug=True)
        return ret
    else:
        return 1


def rootTable(table):
    '''
    Find the root table name, if none, return itself
    LIMITATION: ONLY search 1 upper inherit table
    ex. if tb_child ---inherit--> tb_parent ---inherit--> tb_result
    then rootTable(tb_child) only return tb_parent instead of tb_result
    '''
    schema_file = '{0}_schema'.format(table)
    with open(schema_file) as file_:
        raw = file_.readlines()
    
    ind_root = tools.ind_finder('INHERITS (', raw, reverse=True)
    if ind_root == None:
        return table.split('/')[-1]
    else:
        root = raw[-(ind_root+1)][10:-3]
        return root


def cleanup(table, f_type=None, folder=''):
    '''
    '''

    # print("Type: {0}, Table Name: {1}\n".format(f_type, tb_name))
    if f_type:
        table_file = '{0}_{1}'.format(table, f_type)
    else:
        f_type = table.split('_')[-1]
        table_file = table[:]
        table = table.replace('_{0}'.format(f_type), '')

    with open('./{0}/{1}'.format(folder, table_file)) as file_:
        raw = file_.readlines()

    if f_type == 'schema':
        # Locate the CREATE TABLE
        aim = "CREATE TABLE {0} (\n".format(table)
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
        clean_data = [l.replace('hstore', 'text') for l in clean_data]
        return clean_data

    elif f_type == 'data':
        ind_start = tools.ind_finder('COPY {0} ('.format(table), raw)+1 # Begin from next line
        ind_end = tools.ind_finder('\.', raw, reverse=True)
        clean_data = raw[ind_start:-(ind_end+1)] # [ 'string', 'string', ... ]
        return clean_data
    else:
        print("Please enter file type, return nothing...")
        return None


def dumpFileExtract(table, db_dump):
    '''
    '''
    directory = os.path.join(os.getcwd(), 'Output', 'ExtractedFiles')
    print("Directory is:::::: %s" % directory)
    if not os.path.exists(directory):
        os.makedirs(directory)

    db_dump_path = os.path.join('./Input', db_dump)
    pg_schema = pg_restore(table, f_type='schema', dump_name=db_dump_path, folder='Output/ExtractedFiles')
    assert pg_schema == 0
    root = rootTable(os.path.join(directory, table))
    print("Root is:::::: %s" %root)
    print("Table is::::: %s" %table)
    if root != table:
        pg_root_schema = pg_restore(root, f_type='schema', dump_name=db_dump_path, folder='Output/ExtractedFiles')
        assert pg_root_schema == 0

    schema_cleaned = cleanup(root, 'schema', folder='Output/ExtractedFiles')
    # print(''.join(cleanup(root, 'schema'))) # Print full create table script

    table_name = '{0}_data'.format(table)
    if table in ['tb_sandbox_parent_result', 'tb_protocol_request_logs']:
        if pg_restore(table, f_type='data', dump_name=db_dump_path, folder='Output/ExtractedFiles') == 0:
            data_cleaned = cleanup(table, 'data', folder='Output/ExtractedFiles')
            data_cleaned = [tuple(i.split('\t')) for i in data_cleaned]
            # print(data_cleaned[:2])
        else:
            sys.exit("[ERROR] pg_restore on {0} failed, program abort...".format(table_name))

    else: # Required subtable content
        ## Parsing subtables
        data_cleaned = []
        for sub in os.listdir(os.path.join(os.getcwd(), table)):
            # print("table name: %s" % sub)
            sub_data = cleanup(sub, folder=table)
            sub_data = [tuple(i.split('\t')) for i in sub_data]
            data_cleaned.extend(sub_data)
    return schema_cleaned, data_cleaned