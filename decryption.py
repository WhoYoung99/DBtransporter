# Decrypting db_dump.dat(other name is accepted),  
#  and 
# Checking required sys tool including: pg_resotre and sqlite3
import os
import sys
from tools import exec_cmd


def decryptingDump(file_in, file_out, decrypt_db_tool='decrypt_db_tool.py'):
    '''
    If dumping failed, check if "private.pem" exists.
    Normally, file_in = 'db_dump.dat' & file_out = 'db_dump.dat.decrypted'
    '''
    ret = 0
    directory = os.path.join(os.getcwd(), 'Input')
    file_in_path = os.path.join(directory, file_in)
    file_out_path = os.path.join(directory, file_out)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    if os.path.isfile(file_out_path):
        print("[Pass] Decrypted dump file already exist, continue...")
    else:
        command = "python {0} {1} {2}".format(decrypt_db_tool, file_in_path, file_out_path)
        ret = exec_cmd(command, debug=True)
        if ret:
            sys.exit("[ERROR] Unable to decrypt db_dump, program abort...")
        else:
            print("[Pass] Finish db_dump decrypting...")
    return ret


def checkingTools():
    '''
    Check 2 commands if existed: pg_restore & sqlite3
    if do exist -> pass
    if not -> install through cmd
    '''
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

    ## Double check if installation was successful
    if exec_cmd(command_sqlite) or exec_cmd(command_postgre):
        print("Please install required tools before running the converter again.")
        sys.exit("Bye...")
    else:
        print("[Pass] Required tools checking...")
    return ret


def readingConfigTable(file_name='ConfigTable.dat'):
    file_name_path = os.path.join(os.getcwd(), 'Input', file_name)
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