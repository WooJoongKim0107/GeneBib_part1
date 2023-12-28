from mypathlib import PathTemplate
from . import START, STOP
from replicas import Replica


_R_FILE = PathTemplate('$pdata/epo/epoglobal_2023autumn_$number.pkl.gz')
_W_FILES = {'replica': PathTemplate('$pdata/epo/epo_replicas.pkl'),
            'prints': PathTemplate('$pdata/epo/epo_replicas.txt')}


class EpoReplica(Replica):
    R_FILE = PathTemplate('$pdata/epo/epoglobal_2023autumn_$number.pkl.gz')
    LOAD_PATH = PathTemplate('$pdata/epo/epo_replicas.pkl').substitute()
    W_FILE = PathTemplate('$pdata/epo/epo_replicas.txt').substitute()
    START = START
    STOP = STOP
    KEY_ATTR = 'pub_number'
    NUM_WORKERS = 10


main = EpoReplica.main


if __name__ == '__main__':
    main()
