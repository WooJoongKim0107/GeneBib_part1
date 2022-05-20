from mypathlib import PathTemplate
from . import START, STOP
from .main import W_FILE as R_FILE
from Papers.replicas import Replica


W_FILES = {'replica': PathTemplate('$rsrc/pdata/patent/patent_replicas.pkl'),
           'prints': PathTemplate('$rsrc/pdata/patent/patent_replicas.txt')}


class PatentReplica(Replica):
    R_FILE = R_FILE
    W_FILE = W_FILES['replica']
    START = START
    STOP = STOP
    KEY_ATTR = 'pub_number'


if __name__ == '__main__':
    q = PatentReplica(load=False)
    q.dump()
    with open(W_FILES['prints'].substitute(), 'w', encoding='UTF-8') as file:
        file.write(q.full_comparison())
