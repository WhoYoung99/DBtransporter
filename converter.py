#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
import re
import os
import sys
import subprocess

class DBTable:
    def __init__(self, tb_name):
        self.name = tb_name

    def __repr__(self):
        return '{0.name!r}'.format(self)

    def __str__(self):
        return 'Table name: {0.name!r}'.format(self)

def exec_cmd(cmd, debug=False):
    ret = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
    return ret


def postgre2sqlite(f_name):
    f_type = f.replace('_', '.').split('.')[-2]
    if f_type == 'schema':
        do somthing
    elif f_type == 'data':
        do somthing
    else:
        print("Cannot detect file type, doing general cleanning now...")
        


def export_db(tb_name, dump_name):
    if os.path.isfile(dump_name):
        call_schema = "pg_restore -t {0} -s -f {0}_schema.sql {1}".format(tb_name, dump_name)
        call_data = "pg_restore -t {0} -a -f {0}_data.sql {1}".format(tb_name, dump_name)
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
        command = "python decrypt_db_tool.py"
        decrypt = exec_cmd(command)
        print("[Pass] Finish db_dump decryption...")
    else:
        print("[Pass] Decrypted dump file already exist...")

    #
    ### CHECK REQUIRED TOOLS ###
    ### postgresql-client-9.5
    ### SQLite3
    # 
    command_postgre = "which pg_restore"
    ret = exec_cmd(command_postgre)
    if ret:
        print("Installing postgresql client package...") 
        exec_cmd("sudo apt-get install postgresql-client-9.5")

    command_sqlite = "which sqlite3"
    ret = exec_cmd(command_sqlite)
    if ret:
        print("Installing SQLite3 package...")
        exec_cmd("sudo apt-get install sqlite3")

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
    # doing export_db(tb_name, dump_name)
    #
    # doing tb_file cleaning up


if __name__ == '__main__':
    main()