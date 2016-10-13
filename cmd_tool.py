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