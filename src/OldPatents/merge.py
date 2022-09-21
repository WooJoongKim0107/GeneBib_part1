from mypathlib import PathTemplate
from Papers.merge import Merge
from . import START, STOP
from .refine import W_FILE as R_FILE
from .replicas import PatentReplica


W_FILE = PathTemplate('$rsrc/pdata/full_raw_200920/patent_$index.pkl.gz')

Merge.assign_constants(R_FILE, W_FILE, PatentReplica(load=True), 'pub_number', START, STOP, 90)
main = Merge.main


if __name__ == '__main__':
    main()
