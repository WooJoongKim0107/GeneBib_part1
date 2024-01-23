from mypathlib import PathTemplate
from . import START, STOP
from replicas import Replica


_R_FILE = PathTemplate('$pdata/us_patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)  # TODO Jan 23rd
_W_FILES = {'replica': PathTemplate('$pdata/us_patent/patent_replicas.pkl'),
            'prints': PathTemplate('$pdata/us_patent/patent_replicas.txt')}


class UsPatentReplica(Replica):
    R_FILE = PathTemplate('$pdata/us_patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)  # TODO Jan 23rd
    LOAD_PATH = PathTemplate('$pdata/us_patent/patent_replicas.pkl').substitute()
    W_FILE = PathTemplate('$pdata/us_patent/patent_replicas.txt').substitute()
    START = START
    STOP = STOP
    KEY_ATTR = 'pub_number'
    NUM_WORKERS = 10


main = UsPatentReplica.main


if __name__ == '__main__':
    main()
