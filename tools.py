#
#
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

def writing_file(content, fout_name):
    with open(fout_name, 'w') as file_:
        file_.writelines(['{0}'.format(line) for line in content])
    print("Export to: %s" % os.path.join(os.getcwd(), fout_name))