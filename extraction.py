# DUMPING TABLES
# Input:  db_dump.dat
# Output: list of table dump files

import os
import sys
import shutil
import tools


ROOT = ['tb_cav_total_logs', 'tb_tmufe_total_logs', 'tb_sandbox_result', 'tb_protocol_request_logs']
TB = ['tb_cav_logs', 'tb_tmufe_logs', 'tb_sandbox_parent_result', 'tb_protocol_request_logs']
ROOT_DICT = dict(zip(TB, ROOT))


def makeDirectory(config):
    '''
    ./DBtransporter
        |
        |-- Input/
        |
        |-- Output/
        |   |-- ExtractedFiles/
        |
    '''
    mainpy_path = os.path.dirname(os.path.realpath('main.py'))
    if os.path.exists('./Output'):
        print('[Warning] "Output" Directory has already existed.')
        shutil.rmtree('./Output')
    os.makedirs('Output')
    # os.makedirs('Output/ExtractedFiles')
    # if 'tb_cav_logs' in config:
    #     os.makedirs('Output/ExtractedFiles/tb_cav_logs_data')
    # if 'tb_tmufe_logs' in config:
    #     os.makedirs('Output/ExtractedFiles/tb_tmufe_logs_data')
    print('[Pass] "Output" folder is created successfully...')
        

def leafTable(db_dump_decrypted):
    '''
    Find all leaves of table
    '''
    db_dump_decrypted_path = os.path.join(os.getcwd(), 'Input', db_dump_decrypted)
    table_start_with = ['TABLE DATA public tb_cav_logs_', 'TABLE DATA public tb_tmufe_logs_']
    command = "pg_restore -l -f ./Output/TableAll.dat {0}".format(db_dump_decrypted_path)
    ret = tools.exec_cmd(command)
    if ret:
        sys.exits('[ERROR] Cannot create subtable lists...')
    else:
        with open('./Output/TableAll.dat', 'r') as file_:
            raw = file_.readlines()

        leaves = [i.split(' ')[-2] for i in raw for target in table_start_with if (target in i)]
        # i = len(leaves)
        # assert i/2 == int(i/2)

        # folder = 'tb_cav_logs'
        # folder_path = './Output/ExtractedFiles/{0}'.format(folder)
        # if os.path.isdir('{0}'.format(folder_path)) and os.listdir(folder_path) != []:
        #     print('[Warning] Folder already exist, skip dumping leaves...')
        # else:
        #     print('[Process] Start dumping {0}-leaves...'.format(folder))
            
        #     for leaf in leaves[:int(i/2)]:
        #         ret = pg_restore(leaf, 'data', db_dump_decrypted_path, folder=folder_path)
        #         if ret:
        #             print('[ERROR] Failed to pg_restore {0}'.format(leaf))
        #             return ret
                        
        #     print('[Pass] All {0}-leaves are extracted successfully...'.format(folder))
        # return ret
        

        # folder = 'tb_tmufe_logs'
        # folder_path = './Output/ExtractedFiles/{0}'.format(folder)
        # if os.path.isdir('{0}'.format(folder)) and os.listdir(folder) != []:
        #     print('[Warning] Folder already exist, skip dumping leaves...')
        # else:
        #     print('[Process] Start dumping {0}-leaves...'.format(folder))
        #     for leaf in leaves[int(i/2):]:
        #         ret = pg_restore(leaf, 'data', db_dump_decrypted_path, folder=folder)
        #         if ret:
        #             print('[ERROR] Failed to pg_restore {0}'.format(leaf))
        #             return ret
        ## Logging
        write_leaf = '\n'.join(leaves)
        with open('./Output/TableList.dat', 'w') as file_:
            file_.write(write_leaf)

        return leaves

# def readingConfigTable(file_name='ConfigTable.dat'):
#     file_name_path = os.path.join(os.getcwd(), 'Input', file_name)
#     print(file_name_path)
#     if not os.path.isfile(file_name_path):
#         sys.exit('[ERROR] Cannot find file {0}, program abort...'.format(file_name_path))
#     else:
#         with open(file_name_path, 'r') as file_:
#             tables = file_.read().split()
#             if tables == []:
#                 sys.exit('[ERROR] {0} is empty, program abort...'.format(file_name))
#             else:
#                 print("[Pass] Load ConfigTable.dat...")
#                 return tables

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


# def cleanup(table, f_type=None, folder=''):
#     '''
#     '''

#     # print("Type: {0}, Table Name: {1}\n".format(f_type, tb_name))
#     if f_type:
#         table_file = '{0}_{1}'.format(table, f_type)
#     else:
#         f_type = table.split('_')[-1]
#         table_file = table[:]
#         table = table.replace('_{0}'.format(f_type), '')

#     with open('{0}/{1}'.format(folder, table_file)) as file_:
#         raw = file_.readlines()

#     if f_type == 'schema':
#         # Locate the CREATE TABLE
#         aim = "CREATE TABLE {0} (\n".format(table)
#         ind_start = raw.index(aim)
#         ind_end = raw.index(");\n")
#         # Retrieve only CREATE TABLE part
#         clean_data = raw[ind_start:ind_end+1]
#         # Remove CONSTRAINT
#         if clean_data[-2].startswith('    CONSTRAINT '):
#             del clean_data[-2]
#             clean_data[-2] = ''.join(clean_data[-2].split(',')) # Delete the comma
#         # Remove brackets
#         clean_data = [l.replace('[]', '') for l in clean_data]
#         clean_data = [l.replace('hstore', 'text') for l in clean_data]
#         return clean_data

#     elif f_type == 'data':
#         ind_start = tools.ind_finder('COPY {0} ('.format(table), raw)+1 # Begin from next line
#         ind_end = tools.ind_finder('\.', raw, reverse=True)
#         clean_data = raw[ind_start:-(ind_end+1)] # [ 'string', 'string', ... ]
#         return clean_data
#     else:
#         print("Please enter file type, return nothing...")
#         return None


# def dumpFileExtract(table, db_dump):
#     '''
#     '''
#     directory = os.path.join(os.getcwd(), 'Output', 'ExtractedFiles')
    
#     if not os.path.exists(directory):
#         os.makedirs(directory)

#     db_dump_path = os.path.join('./Input', db_dump)
#     pg_schema = pg_restore(table, f_type='schema', dump_name=db_dump_path, folder='Output/ExtractedFiles')
#     assert pg_schema == 0
#     root = rootTable(os.path.join(directory, table))
    
#     if root != table:
#         pg_root_schema = pg_restore(root, f_type='schema', dump_name=db_dump_path, folder='Output/ExtractedFiles')
#         assert pg_root_schema == 0

#     schema_cleaned = cleanup(root, 'schema', folder='Output/ExtractedFiles')
#     # print(''.join(cleanup(root, 'schema'))) # Print full create table script

#     table_name = '{0}_data'.format(table)
#     if table in ['tb_sandbox_parent_result', 'tb_protocol_request_logs']:
#         if pg_restore(table, f_type='data', dump_name=db_dump_path, folder='Output/ExtractedFiles') == 0:
#             data_cleaned = cleanup(table, 'data', folder='Output/ExtractedFiles')
#             data_cleaned = [tuple(i.split('\t')) for i in data_cleaned]
#             # print(data_cleaned[:2])
#         else:
#             sys.exit("[ERROR] pg_restore on {0} failed, program abort...".format(table_name))

#     else: # Required subtable content
#         ## Parsing subtables
#         data_cleaned = []
#         for sub in os.listdir(os.path.join(os.getcwd(), table)):
#             # print("table name: %s" % sub)
#             sub_data = cleanup(sub, folder=table)
#             sub_data = [tuple(i.split('\t')) for i in sub_data]
#             data_cleaned.extend(sub_data)
#     return schema_cleaned, data_cleaned


def dumpFileExtract(table, db_dump, leaves, outpath='./Output/ExtractedFiles'):
    '''
    Input: 
        > table     - table name in Postgresql
        > outpath   - full path, location of output files
    Output:
        > return    - None
        > create    - the schema & data files for the specific table
    '''
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    db_dump_path = os.path.join('./Input', db_dump)
    seperate_ind = int(len(leaves)/2)
    cav_leaves = leaves[:seperate_ind]
    tmufe_leaves = leaves[seperate_ind:]
    ### extract Schema
    pg_schema = pg_restore(table, f_type='schema', dump_name=db_dump_path, folder=outpath)
    orig_name = '{0}_schema'.format(table)
    print('[Debug] Create schema file: {0}'.format(orig_name))
    assert pg_schema == 0
    ### find root Schema
    root = rootTable(os.path.join(outpath, table))
    if root != table:
        pg_root_schema = pg_restore(root, f_type='schema', dump_name=db_dump_path, folder='Output/ExtractedFiles')
        assert pg_root_schema == 0
        os.remove(os.path.join(outpath, orig_name))
        # os.rename(os.path.join(outpath, '{0}_schema'.format(root)), os.path.join(outpath, orig_name))
    
    ### extract Data
    if table in ['tb_sandbox_parent_result', 'tb_protocol_request_logs']:
        pg_data = pg_restore(table, f_type='data', dump_name=db_dump_path, folder=outpath)
    elif table in ['tb_cav_logs', 'tb_tmufe_logs']:

        if table == 'tb_cav_logs':
            arg = makeArg('tb_cav_logs_data', cav_leaves, db_dump_path)
        elif table == 'tb_tmufe_logs':
            arg = makeArg('tb_tmufe_logs_data', tmufe_leaves, db_dump_path)
        pg_data = tools.exec_cmd(arg, debug=True)


        # outpath_sub = os.path.join(outpath, table+'_data')
        # if os.listdir(outpath_sub) == []:
        #     print('[Process] Start dumping {0}-leaves...'.format(table))
        #     if table == 'tb_cav_logs':
        #         # pg_data = sum([pg_restore(leaf, f_type='data', dump_name=db_dump_path, folder=outpath_sub) for leaf in cav_leaves])
        #         arg = makeArg('tb_cav_logs_data', cav_leaves, db_dump_path)
        #         pg_data = tools.exec_cmd(arg, debug=True)
        #     elif table == 'tb_tmufe_logs':
        #         arg = makeArg('tb_tmufe_logs_data', tmufe_leaves, db_dump_path)
        #         pg_data = tools.exec_cmd(arg, debug=True)
        #         # pg_data = sum([pg_restore(leaf, f_type='data', dump_name=db_dump_path, folder=outpath_sub) for leaf in tmufe_leaves])
        #     assert pg_data == 0
        # else:
        #     pg_data = -1
        #     print('[Warning] {0} folder is not empty, skip dumping leaves...'.format(table))
    else:
        pg_data = -99
        print('[ERROR] Unrecognized table name: {0}'.format(table))
    return pg_schema + pg_data
        

def makeArg(name, leaves, db_dump):
    command = ['pg_restore']
    for leaf in leaves:
        command.append('-t')
        command.append(leaf)
    command.extend(['-a', '-f', './Output/ExtractedFiles/{0}'.format(name), db_dump])
    return ' '.join(command)



