from mypathlib import PathTemplate
from Papers.merge import Merge
from . import START, STOP
from .refine import W_FILE as R_FILE
from .replicas import PatentReplica


W_FILE = PathTemplate('$rsrc/pdata/full_raw_200920/patent_$index.pkl.gz')


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
