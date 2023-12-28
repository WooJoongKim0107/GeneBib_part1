from mypathlib import PathTemplate
from Papers.merge import Merge
from . import START, STOP
from .replicas import UsPatentReplica


R_FILE = PathTemplate('$pdata/us_patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)
_R_FILE = PathTemplate('$pdata/us_patent/patent_replicas.pkl')
W_FILE = PathTemplate('$pdata/us_patent/patent_$index.pkl.gz')

Merge.assign_constants(R_FILE, W_FILE, UsPatentReplica.load(), 'pub_number', START, STOP, 90)  # Read
main = Merge.main


if __name__ == '__main__':
    main()
