# DUMPING TABLES
# Input:  db_dump.dat
# Output: list of table dump files

import os
from func_tools import exec_cmd

def root_table(table_name):
    '''
    Find the root table name, if none, return itself
    LIMITATION: ONLY search 1 upper inherit table
    ex. if tb_child ---inherit--> tb_parent ---inherit--> tb_result
    then root_table(tb_child) only return tb_parent instead of tb_result
    '''
    from func_tools import ind_finder
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
            print('[Warning] Folder already exist, skip dumping leaves...')
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
            print('[Warning] Folder already exist, skip dumping leaves...')
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