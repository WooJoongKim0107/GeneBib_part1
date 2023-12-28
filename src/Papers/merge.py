from mypathlib import PathTemplate
from merge import Merge
from . import START, STOP
from .replicas import PaperReplica


R_FILE = PathTemplate('$pdata/paper/article22n$number.pkl.gz', key='{:0>4}'.format)
_R_FILE0 = PathTemplate('$pdata/paper/paper_replicas.pkl')
W_FILE = PathTemplate('$pdata/paper/paper_$index.pkl.gz')


Merge.assign_constants(R_FILE, W_FILE, PaperReplica(), 'pmid', START, STOP, 10)
main = Merge.main

if __name__ == '__main__':
    main()
