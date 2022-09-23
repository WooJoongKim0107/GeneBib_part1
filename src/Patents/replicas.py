from mypathlib import PathTemplate
from . import START, STOP
from Papers.replicas import Replica


_R_FILE = PathTemplate('$rsrc/pdata/patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)
_W_FILES = {'replica': PathTemplate('$rsrc/pdata/patent/patent_replicas.pkl'),
            'prints': PathTemplate('$rsrc/pdata/patent/patent_replicas.txt')}


class PatentReplica(Replica):
    R_FILE = PathTemplate('$rsrc/pdata/patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)
    LOAD_PATH = PathTemplate('$rsrc/pdata/patent/patent_replicas.pkl').substitute()
    W_FILE = PathTemplate('$rsrc/pdata/patent/patent_replicas.txt').substitute()
    START = START
    STOP = STOP
    KEY_ATTR = 'pub_number'


main = PatentReplica.main


if __name__ == '__main__':
    main()
