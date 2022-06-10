from mypathlib import PathTemplate
from Papers.merge import Merge
from . import START, STOP
from .refine import W_FILE as R_FILE
from .replicas import PaperReplica


W_FILE = PathTemplate('$rsrc/pdata/pubmed20n_gz/paper_$index.pkl.gz')
replicas = PaperReplica(load=True)


Merge.R_FILE = R_FILE
Merge.W_FILE = W_FILE
Merge.REPLICAS = PaperReplica(load=True)
Merge.START = START
Merge.STOP = STOP
Merge.STEP = 10
main = Merge.main


if __name__ == '__main__':
    main()
