from mypathlib import PathTemplate
from Papers.merge import Merge
from . import START, STOP
from .replicas import PatentReplica


R_FILE = PathTemplate('$rsrc/pdata/patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)
W_FILE = PathTemplate('$rsrc/pdata/patent/patent_$index.pkl.gz')


Merge.R_FILE = R_FILE
Merge.W_FILE = W_FILE
Merge.REPLICAS = PatentReplica(load=True)
Merge.KEY_ATTR = 'pub_number'
Merge.START = START
Merge.STOP = STOP
Merge.STEP = 90
main = Merge.main


if __name__ == '__main__':
    main()
