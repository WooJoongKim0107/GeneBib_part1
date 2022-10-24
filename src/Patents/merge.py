from mypathlib import PathTemplate
from Papers.merge import Merge
from . import START, STOP
from .replicas import PatentReplica


R_FILE = PathTemplate('$pdata/patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)
_R_FILE = PathTemplate('$pdata/patent/patent_replicas.pkl')
W_FILE = PathTemplate('$pdata/patent/patent_$index.pkl.gz')

Merge.assign_constants(R_FILE, W_FILE, PatentReplica.load(), 'pub_number', START, STOP, 90)  # Read
main = Merge.main


if __name__ == '__main__':
    main()
