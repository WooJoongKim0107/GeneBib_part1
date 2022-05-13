from mypathlib import PathTemplate
from .main import W_FILE as R_FILE
from Papers.replicas import Replica


W_FILES = {'replica': PathTemplate('$rsrc/pdata/patent/patent_replicas.pkl'),
           'prints': PathTemplate('$rsrc/pdata/patent/patent_replicas.txt')}


class PatentReplica(Replica):
    Replica.R_FILE = R_FILE
    Replica.W_FILE = W_FILES['replica']
    Replica.START = 0
    Replica.STOP = 10033
    Replica.ID_ATTR = 'pub_number'


if __name__ == '__main__':
    q = PatentReplica(load=False)
    q.dump()
    with open(W_FILES['prints'].substitute(), 'w', encoding='UTF-8') as file:
        file.write(q.full_comparison())
