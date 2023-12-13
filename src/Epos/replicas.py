from mypathlib import PathTemplate
from . import START, STOP
from Papers.replicas import Replica


_R_FILE = PathTemplate('$pdata/epo/epo_202111_$number.pkl.gz', key='{:0>12}'.format)
_W_FILES = {'replica': PathTemplate('$pdata/epo/epo_replicas.pkl'),
            'prints': PathTemplate('$pdata/epo/epo_replicas.txt')}


class EpoReplica(Replica):
    R_FILE = PathTemplate('$pdata/epo/epo_202111_$number.pkl.gz', key='{:0>12}'.format)
    LOAD_PATH = PathTemplate('$pdata/epo/epo_replicas.pkl').substitute()
    W_FILE = PathTemplate('$pdata/epo/epo_replicas.txt').substitute()
    START = START
    STOP = STOP
    KEY_ATTR = 'epo_number'


main = EpoReplica.main


if __name__ == '__main__':
    main()
