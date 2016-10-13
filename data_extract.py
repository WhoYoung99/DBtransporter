import os
from cmd_tool import exec_cmd
from data_dump import ind_finder

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
        ret = exec_cmd(call, debug=True)
        return ret
    else:
        return 1


def root_table(schema_file):
    '''
    Find the root table name, if none, return itself
    LIMITATION: ONLY search 1 upper inherit table
    ex. if tb_child ---inherit--> tb_parent ---inherit--> tb_result
    then root_table(tb_child) only return tb_parent instead of tb_result
    '''
    with open(schema_file) as file_:
        raw = file_.readlines()
    
    ind_root = ind_finder('INHERITS (', raw, reverse=True)
    if ind_root == None:
        return schema_file
    else:
        root = raw[-(ind_root+1)][10:-3]
        schema_file_root = '{0}_schema'.format(root)
        return schema_file_root


def cleanup(f_name, folder=''):
    '''
    f_name: blabla_blablabla_..._schema/data
    '''
    tb_name = '_'.join(f_name.split('_')[:-1]) # delete _schema & _data
    f_type = f_name.replace('_', '.').split('.')[-1]
    # print("Type: {0}, Table Name: {1}\n".format(f_type, tb_name))
    with open('./{0}/{1}'.format(folder, f_name)) as file_:
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
        clean_data = [l.replace('hstore', 'text') for l in clean_data]
        return clean_data

    elif f_type == 'data':
        # ind_start = ['COPY {0} ('.format(tb_name) in l for l in raw].index(True)
        ind_start = ind_finder('COPY {0} ('.format(tb_name), raw)+1 # Begin from next line
        ind_end = ind_finder('\.', raw, reverse=True)
        clean_data = raw[ind_start:-(ind_end+1)] # [ 'string', 'string', ... ]
        return clean_data
    else:
        print("Cannot detect file type, do it on your own...")
        return None


def dumpFileExtract(table, db_dump):
    table_name = '{0}_schema'.format(table)
    pg_schema = pg_restore(table, f_type='schema', dump_name=db_dump)
    assert pg_schema == 0
    root_name = root_table(table_name)
    if root_name != table_name:
        pg_root_schema = pg_restore(root_name, f_type='schema', dump_name=db_dump)
        assert pg_root_schema == 0

    schema_cleaned = cleanup(root_name)
    print(''.join(cleanup(root_name))) # Print full create table script

    table_name = '{0}_data'.format(table)
    if table in ['tb_sandbox_parent_result', 'tb_protocol_request_logs']:
        if pg_restore(table, f_type='data', dump_name=db_dump) == 0:
            data_cleaned = cleanup(table_name)
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
            data_cleaned += sub_data

    return schema_cleaned, data_cleaned