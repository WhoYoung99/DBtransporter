# import decryption
import extraction
# import restortion
# import conversion

import re
import os
import pprint as pp
import subprocess
import dbmanager

tables = ['tb_sandbox_parent_result', 'tb_cav_logs', 'tb_tmufe_logs', 'tb_protocol_request_logs']
roots = ['tb_sandbox_result', 'tb_cav_total_logs', 'tb_tmufe_total_logs', 'tb_protocol_request_logs']
tableroot_mapping = dict(zip(tables, roots))

CONFIG = ['tb_sandbox_parent_result', 'tb_cav_logs', 'tb_tmufe_logs', 'tb_protocol_request_logs']
DB_DUMP = './Input/db_dump_decrypted.dat'


#################### EXTRACTION #################################
#   Input:  CONFIG, full path of DB_DUMP
#   Output: extract_schemas, extract_datas
#################################################################

creat_folder = extraction.makeDirectory(tables)
flag_cav, flag_tmufe = [i in CONFIG for i in ['tb_cav_logs', 'tb_tmufe_logs']]

if any([flag_cav, flag_tmufe]):
    cmd_listalltable = ['pg_restore', '-l', DB_DUMP]
    raw_subtable = subprocess.run(cmd_listalltable, stdout=subprocess.PIPE).stdout.decode()
    pattern_datelist = re.compile(r'TABLE DATA public tb_tmufe_logs_(\d+_\d+_\d+)', re.DOTALL)
    pattern_subtable = re.compile(r'(\s\d+-\d+-\d+ \d+:\d+:\d+\s.*)') # Latter usage
    datelist = pattern_datelist.findall(raw_subtable)

cmd_schema_draft = ['pg_restore'] + ['-t {0}'.format(tableroot_mapping[table]) for table in CONFIG] + ['-s {0}'.format(DB_DUMP)]
raw_schema = subprocess.run(' '.join(cmd_schema_draft).split(' '), stdout=subprocess.PIPE).stdout.decode()
pattern_schemas = re.compile(r'CREATE TABLE (\w+)\s\S\s(.*?)\)\;', re.DOTALL)
extract_schemas = pattern_schemas.findall(raw_schema)
'''
Input:  string = subprocess.run([ 'pg_restore', '-t', tablename, ...], stdout=subprocess.PIPE).decode()
Output: out = pattern_schema.findall(Input)
        type(out) = <class 'list'>
        out[0] = ('tb_cav_total_logs', '    __log_time timestamp without time zone NOT NULL,\n    
                                            __log_utc_time timestamp without time zone NOT NULL,\n    
                                            threattype integer,\n    
                                            protocolgroup integer,\n    
                                            protocol integer,\n    
                                            vlanid integer,\n    
                                            direction integer,\n    
                                            dstip inet,\n    
                                            
                                            ...

                                            ori_id bigint,\n
                                            cveid character varying(64)[],\n
                                            apt_campaign character varying(64)[],\n
                                            http_referer character varying(2048),\n
                                            clientflag integer DEFAULT 0\n);')

        out[1] = ('tb_protocol_request_logs',   '    protocolgroup character varying(512),\n
                                                    trafficprocessed bigint,\n
                                                    _ hstore,\n
                                                    starttime timestamp without time zone,\n
                                                    endtime timestamp without time zone\n);')
        out[2] = ('tb_sandbox_result', "    id integer NOT NULL,\n
                                            receivedtime timestamp without time zone NOT NULL,\n
                                            sha1 character(40),\n
                                            severity integer,\n

                                            ...

                                            pcapready integer,\n
                                            dropsha1list character(40)[],\n
                                            virusname character varying(1024),\n
                                            va_threat_category_ids integer[],\n
                                            va_virusname_list character varying(1024)[],\n
                                            CONSTRAINT tb_sandbox_result_length_check CHECK ((NOT ((char_length(array_to_string(va_virusname_list, ','::text)) > 2048) OR (char_length(array_to_string(va_threat_category_ids, ','::text)) > 128))))\n);")

        out[3]=('tb_tmufe_total_logs', '    __log_time timestamp without time zone NOT NULL,\n
                                            __log_utc_time timestamp without time zone NOT NULL,\n
                                            threattype integer DEFAULT 5,\n
                                            protocolgroup integer,\n
                                            protocol integer,\n
                                            
                                            ...

                                            ccca_detectionsource integer,\n
                                            _ hstore,\n
                                            __log_id bigint NOT NULL,\n
                                            ece_severity smallint,\n
                                            attack_phase character varying(256),\n

                                            ...

                                            cveid character varying(64)[],\n
                                            common_threat_family character varying(256),\n
                                            apt_related smallint DEFAULT 0,\n
                                            apt_group character varying(512),\n
                                            apt_campaign character varying(64)[],\n
                                            http_referer character varying(2048),\n
                                            clientflag integer DEFAULT 0\n);')
'''

### Order needs to be adjust ###
sqlite_datas = list()
################################

if flag_cav:
    CONFIG.remove('tb_cav_logs')
    cmd_data_cav = ['pg_restore'] + ['-t tb_cav_logs_{0}'.format(date) for date in datelist] + ['-a {0}'.format(DB_DUMP)]
    raw_data_cav = subprocess.run(' '.join(cmd_data_cav).split(' '), stdout=subprocess.PIPE).stdout.decode()
    subtable_data_cav = [tuple(table.split('\t')) for table in pattern_subtable.findall(raw_data_cav)]
    sqlite_datas.append(tuple(['tb_cav_logs', subtable_data_cav]))
    '''
    >>> print(subtable_cav[0]):
    ('\n2016-05-25 17:24:07',
     '2016-05-25 09:24:07',
     '1',
     '6',
     '5',
     '4095',
     '0',
     '221.130.179.36',
     '80',
     '38:2c:4a:65:bc:64',
     '192.168.1.228',
     '56270',
     'c8:60:00:cb:14:c7',
     '',
     'js001.3322.org',
     '',
     '1',
     'MALWARE',
     '',
     '',
     '0',
     '0',
     '1536',
     'HTTP Request to a malware Command and Control Site',
     '1',
     '',
     '',
     '',
     '',
     '',
     '',
     '',
     'http://js001.3322.org/',
     '',
     '0',
     'AutoIt',
     '',
     '43',
     '1',
     '0',
     '',
     '',
     '0',
     '221.130.179.36',
     'tw-walttsai3',
     'a2603aba-f0f8-4837-8397-265994439832',
     '4',
     'Default',
     '1',
     '',
     '0',
     '1',
     '0',
     '0',
     '0',
     '',
     '',
     '',
     '',
     '\\N',
     '\\N',
     '\\N',
     '\\N',
     '\\N',
     '',
     '\\N',
     '',
     '192.168.1.228',
     '221.130.179.36',
     '0',
     '\\N',
     '0',
     '1',
     '1',
     '',
     '',
     '0000000000000000000000000000000000000000',
     '1',
     '3',
     '2',
     'http://js001.3322.org/',
     '0',
     '\\N',
     '\\N',
     '\\N',
     '\\N',
     '',
     '216301',
     '8',
     'Command and Control Communication',
     '3',
     '["URL: http://js001.3322.org/"]',
     'http://js001.3322.org/',
     '3',
     '\\N',
     'Bot',
     '\\N',
     '0',
     'Callback',
     '\\N',
     '',
     '0',
     '\\N',
     '1536',
     '0',
     '\\N',
     '\\N',
     '\\N',
     '\\N',
     '\\N',
     '\\N',
     '\\N',
     '\\N',
     '',
     '0')
    >>> print(len(subtable_cav[0])):
    115 (same as column numbers)
    '''

if flag_tmufe:
    CONFIG.remove('tb_tmufe_logs')
    cmd_data_tmufe = ['pg_restore'] + ['-t tb_tmufe_logs_{0}'.format(date) for date in datelist] + ['-a {0}'.format(DB_DUMP)]
    raw_data_tmufe = subprocess.run(' '.join(cmd_data_tmufe).split(' '), stdout=subprocess.PIPE).stdout.decode()
    subtable_data_tmufe = [tuple(table.split('\t')) for table in pattern_subtable.findall(raw_data_tmufe)]
    sqlite_datas.append(tuple(['tb_tmufe_logs', subtable_data_tmufe]))

cmd_data_draft = ['pg_restore'] + ['-t {0}'.format(table) for table in CONFIG] + ['-a {0}'.format(DB_DUMP)]
raw_data = subprocess.run(' '.join(cmd_data_draft).split(' '), stdout=subprocess.PIPE).stdout.decode()
extract_datas = raw_data.split('\n')

#################### RESTORATION #################################
#   Input:  extract_schemas, datas
#   Output: tables in SQLite database
#################################################################
db_path = './Output/TDADB.db'
if os.path.isfile(db_path): os.remove(db_path)
db = dbmanager.DatabaseManager('TDADB.db')


def postgre2sqliteSchema(script):
    script = [line.replace('[]', '') for line in script]
    if script[-2].startswith('    CONSTRAINT '):
        del script[-2]
        script[-2] = script[-2].strip(',')
    return tuple(script)

def dataSlicer(script):
    assert type(script) == list
    collector = []
    slice_ind = []
    for ind, line in enumerate(script):
        if line.startswith('COPY '):
            table = line[5:29]
            collector.append(table)
            head = ind + 1
        if line.startswith('\.'):
            tail = ind
            slice_ind.append(tuple((head, tail)))
    return list(zip(collector, slice_ind))


create_schema_script = lambda table, columns: tuple('CREATE TABLE {0} (\n{1});'.format(table, columns).split('\n'))
postgre_schemas = [create_schema_script(table[0], table[1]) for table in extract_schemas]
sqlite_schemas  = [postgre2sqliteSchema(s) for s in postgre_schemas]
db.create(sqlite_schemas)



indicator_list = dataSlicer(extract_datas)
for table, (ind_head, ind_tail) in indicator_list:
    chunk_data  = extract_datas[ind_head:ind_tail]
    column_data = [tuple(i.split('\t')) for i in chunk_data]
    sqlite_datas.append(tuple([table, column_data]))

db.restore(sqlite_datas)