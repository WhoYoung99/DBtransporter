# Decrypting db_dump.dat(other name is accepted),  
#  and 
# Checking required sys tool including: pg_resotre and sqlite3
import os
import sys
import subprocess

def exec_cmd(cmd, debug=False):
    '''
    ret == 0 : success
    ret == 1 : fail
    '''
    if debug:
        ret = subprocess.call(cmd, shell=True)
    else:
        ret = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
    return ret


def decryptingDump(file_in, file_out, decrypt_db_tool='decrypt_db_tool.py'):
    '''
    If dumping failed, check if "private.pem" exists.
    Normally, file_in = 'db_dump.dat' & file_out = 'db_dump.dat.decrypted'
    '''
    ret = 0
    if os.path.isfile(file_out):
        print("[Pass] Decrypted dump file already exist, continue...")
    else:
        command = "python {0} {1} {2}".format(decrypt_db_tool, file_in, file_out)
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