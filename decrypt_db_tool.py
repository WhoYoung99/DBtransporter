#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import traceback
import subprocess
import os
import time

DIR_TMP = './'

DB_DATA_MAGIC_NUMBER = '9090'
CYPHER_VERSION_LENGTH = 128
CYPHER_PASSWORD_LENGTH = 128
CHUNK_SIZE = 65536
PATH_VERSION_DEC = DIR_TMP + '/version.dec'
PATH_PASSWORD_DEC = DIR_TMP + '/passwd.dec'
PATH_PRIVATE_PEM = 'private.pem'
TEMP_DB_NAME = "TEMP_TDADB"

ERROR_MAIN_FUNC                     = 0x01
ERROR_DOWNLOAD_STREAM               = 0x40
ERROR_MAGIC_NUMBER                  = 0x50
ERROR_DECRYPT_VERSION               = 0x60
ERROR_PRIVATE_KEY_DOES_NOT_EXIST    = 0x64
ERROR_VERSION_IS_DIFFERENT          = 0x68
ERROR_BUILD_IS_DIFFERENT            = 0x69
ERROR_DECRYPT_PASSWORD              = 0x70
ERROR_SEMLOCK_TIMEOUT               = 0x80
ERROR_DROP_AND_RESTORE_DATABASE     = 0x90
ERROR_DROP_DATABASE                 = 0x94
ERROR_GET_DB_NAME                   = 0x98
ERROR_IS_DB_EXIST                   = 0x99
ERROR_RESTORE_DATABASE              = 0x9C
ERROR_DECRYPT_DATABASE              = 0xA8
ERROR_PASSWD_DEC_DOES_NOT_EXIST     = 0xA9
ERROR_IMPORT_FILE_IS_INCOMPLETE     = 0xAA
ERROR_REPAIR_DATABASE               = 0xAC
ERROR_SEND_SYSTEM_EVENT_LOG         = 0xC0
ERROR_UNKNOWN_ERROR_CODE            = 0xFF

def exec_cmd(cmd):
    ret = subprocess.call(cmd, shell=True)
    print('[%s] ret = %s' % (cmd, ret) )

def check_output(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]

def decrypt_version(pwfile, version_dec_path):
    print('========decrypt_version Start========')
    ret = 0
    try:
        if not os.path.isfile(PATH_PRIVATE_PEM):
            print('========private key does not exist========')
            return ERROR_PRIVATE_KEY_DOES_NOT_EXIST
        
        with open(version_dec_path, 'w') as version_file:
            cmd = 'openssl rsautl -inkey {0} -decrypt'.format(PATH_PRIVATE_PEM)
            rsautl_dec = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=version_file)
            rsautl_dec.communicate(pwfile.read(CYPHER_VERSION_LENGTH))
            rsautl_dec.wait()
            version_file.flush()
            rsautl_dec.stdin.close()
        with open(version_dec_path, 'r') as version_file:
            export_build = version_file.readline()
            print('export_build = %s' % export_build)

    except:
        error_string = traceback.format_exc()
        return ERROR_DECRYPT_VERSION
    finally:
        if os.path.isfile(version_dec_path):
            os.remove(version_dec_path)
    print('========decrypt_version End  ========')
    return ret

def decrypt_password(pwfile, pw_dec_path):
    print('========decrypt_password Start========')
    ret = 0
    try:
        if not os.path.isfile(PATH_PRIVATE_PEM):
            print('========private key does not exist========')
            return ERROR_PRIVATE_KEY_DOES_NOT_EXIST

        with open(pw_dec_path, 'w') as pw_dec_file:
            cmd = 'openssl rsautl -inkey {0} -decrypt'.format(PATH_PRIVATE_PEM)
            rsautl_dec = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=pw_dec_file)
            rsautl_dec.communicate(pwfile.read(CYPHER_PASSWORD_LENGTH))
            rsautl_dec.wait()
            pw_dec_file.flush()
            rsautl_dec.stdin.close()
    except:
        error_string = traceback.format_exc()
        print(error_string)
        return ERROR_DECRYPT_PASSWORD
    print('========decrypt_password End  ========')
    return ret

def decrypt_database(dbfile, pw_dec_path, file_out):
    print('========decrypt_database Start========')
    ret = 0
    try:
        if not os.path.isfile(pw_dec_path):
            print('========%s does not exist========' % pw_dec_path)
            return ERROR_PASSWD_DEC_DOES_NOT_EXIST

        #cmd = 'openssl enc -d -aes-256-cbc -a -salt -pass file:{0} | xz -d -c -0 | /usr/bin/pg_restore -d \"{1}\"'.format(pw_dec_path, TEMP_DB_NAME)
        cmd = 'openssl enc -d -aes-256-cbc -a -salt -pass file:{0} | xz -d -c -0 > {1}'.format(pw_dec_path, file_out)
        db_restore = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=None, stderr=None, bufsize=1)
        file_incomplete = 1
        while True:
            chunk_len = dbfile.read(4)
            if not chunk_len:
                print('not chunk_len')
                break
            if chunk_len == "DONE":
                print('chunk_len == DONE')
                file_incomplete = 0
                break
            if len(chunk_len) != 4:
                print('len(chunk_len):%s != 4' % len(chunk_len))
                break
            piece_len = int(chunk_len, 16)
            if piece_len == 0:
                piece_len = CHUNK_SIZE
            else:
                print('last piece_len = [%s]' % piece_len)
            chunk = dbfile.read(piece_len)
            if len(chunk) != piece_len:
                print('len(chunk):%s != piece_len:%s' % (len(chunk), piece_len) )
                break
            db_restore.stdin.write(chunk)
        db_restore.stdin.close()
        print('Waiting db_restore complete ...')
        db_restore.wait()

        if file_incomplete == 1:
            print('========Archive file is incomplete========')
            return ERROR_IMPORT_FILE_IS_INCOMPLETE
    except:
        if db_restore in locals():
            db_restore.stdin.close()
            print('Waiting db_restore complete ...')
            db_restore.wait()
        error_string = traceback.format_exc()
        print(error_string)
        return ERROR_DECRYPT_DATABASE
    finally:
        if os.path.isfile(pw_dec_path):
            os.remove(pw_dec_path)
    print('========decrypt_database End  ========')
    return ret

def drop_database(db_name):
    print('========drop_database %s Start========' % db_name)
    ret = 0
    try:
            exec_cmd('/usr/bin/dropdb TEMP_TDADB -e')

    except:
        error_string = traceback.format_exc()
        print(error_string)
        return ERROR_DROP_DATABASE
    print('========drop_database End  ========')
    return ret

def restore_database(dbfile, pw_dec_path, file_out):
    print('========restore_database Start========')
    ret = 0
    try:
        exec_cmd('/usr/bin/createdb -T template0 \"{0}\"'.format(TEMP_DB_NAME) )

        cmd = '/usr/bin/psql -q -d postgres -c "select datallowconn from pg_database where datname = \'TEMP_TDADB\'"'
        datallowconn = check_output(cmd)
        print('TEMP_TDADB datallowconn = [%s]' % datallowconn)

        ret = decrypt_database(dbfile, pw_dec_path, file_out)

    except:
        error_string = traceback.format_exc()
        print(error_string)
        return ERROR_RESTORE_DATABASE
    print('========restore_database End  ========')
    return ret

def drop_and_restore_database(dbfile, pw_dec_path, file_out):
    print('========drop_and_restore_database Start========')
    ret = 0
    try:
        ret = drop_database(TEMP_DB_NAME)
        if ret != 0:
            return ret

        ret = restore_database(dbfile, pw_dec_path, file_out)
    except:
        error_string = traceback.format_exc()
        print(error_string)
        return ERROR_DROP_AND_RESTORE_DATABASE
    print('========drop_and_restore_database End  ========')
    return ret

def download_stream(db_dump_file, file_out):
    print('========download_stream Start========')
    ret = 0
    try:
        magic_number = db_dump_file.read(4)
        print('magic_number = %s' % magic_number)
        if magic_number != DB_DATA_MAGIC_NUMBER:
            return ERROR_MAGIC_NUMBER

        ret = decrypt_version(db_dump_file, PATH_VERSION_DEC)
        if ret != 0:
            return ret

        ret = decrypt_password(db_dump_file, PATH_PASSWORD_DEC)
        if ret != 0:
            return ret

        ret = drop_and_restore_database(db_dump_file, PATH_PASSWORD_DEC, file_out)

        if ret != 0:
            return ret
    except:
        error_string = traceback.format_exc()
        print(error_string)
        return ERROR_DOWNLOAD_STREAM
    print('========download_stream End  ========')
    return ret

if __name__ == '__main__':
    os.environ['PGAPPNAME'] = 'import_db'
    ret = 0
    file_in = sys.argv[1]
    file_out = sys.argv[2]
    try:
        with open(file_in, 'r') as db_dump_dat:
            ret = download_stream(db_dump_dat, file_out)
    except:
        error_string = traceback.format_exc()
        ret = ERROR_MAIN_FUNC
    finally:
        if 'db_dump_dat' in locals():
            db_dump_dat.close()
    exit(ret)

