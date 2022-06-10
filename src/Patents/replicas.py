from mypathlib import PathTemplate
from . import START, STOP
from .refine import W_FILE as R_FILE
from Papers.replicas import Replica


W_FILES = {'replica': PathTemplate('$rsrc/pdata/patent/patent_replicas.pkl'),
           'prints': PathTemplate('$rsrc/pdata/patent/patent_replicas.txt')}


class PatentReplica(Replica):
    R_FILE = R_FILE
    W_FILES = W_FILES
    START = START
    STOP = STOP
    KEY_ATTR = 'pub_number'


main = PatentReplica.main


if __name__ == '__main__':
    main()
