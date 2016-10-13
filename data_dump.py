# DUMPING TABLES
# Input:  db_dump.dat
# Output: list of table dump files

import os
from cmd_tool import exec_cmd


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


def leaf_table(db_dump_decrypted):
    '''
    Find all leaves of table
    '''
    table_start_with = ['TABLE DATA public tb_cav_logs_', 'TABLE DATA public tb_tmufe_logs_']
    command = "pg_restore -l -f TableAll.dat {0}".format(db_dump_decrypted)
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

def readingConfigTable(file_name='ConfigTable.dat'):
    if not os.path.isfile(file_name):
        sys.exit('[ERROR] Cannot find file {0}, program abort...'.format(file_name))
    else:
        with open(file_name, 'r') as file_:
            tables = file_.read().split()
            if tables == []:
                sys.exit('[ERROR] {0} is empty, program abort...'.format(file_name))
            else:
                print("[Pass] Load ConfigTable.dat...")
                return tables