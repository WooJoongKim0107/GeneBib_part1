from mypathlib import PathTemplate
from replicas import Replica
from . import START, STOP


_R_FILE = PathTemplate('$pdata/paper/article22n$number.pkl.gz', key='{:0>4}'.format)
_W_FILES = {'replica': PathTemplate('$pdata/paper/paper_replicas.pkl'),
            'prints': PathTemplate('$pdata/paper/paper_replicas.txt')}


class PaperReplica(Replica):
    R_FILE = PathTemplate('$pdata/paper/article22n$number.pkl.gz', key='{:0>4}'.format)
    LOAD_PATH = PathTemplate('$pdata/paper/paper_replicas.pkl').substitute()
    W_FILE = PathTemplate('$pdata/paper/paper_replicas.txt').substitute()
    START = START
    STOP = STOP
    KEY_ATTR = 'pmid'


main = PaperReplica.main

if __name__ == '__main__':
    main()
