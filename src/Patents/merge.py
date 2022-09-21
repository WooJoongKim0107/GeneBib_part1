from mypathlib import PathTemplate
from Papers.merge import Merge
from . import START, STOP
from .replicas import PatentReplica


R_FILE = PathTemplate('$rsrc/pdata/patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)
W_FILE = PathTemplate('$rsrc/pdata/patent/patent_$index.pkl.gz')

Merge.assign_constants(R_FILE, W_FILE, PatentReplica(load=True), 'pub_number', START, STOP, 90)
main = Merge.main


if __name__ == '__main__':
    main()
