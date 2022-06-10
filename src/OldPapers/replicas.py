from mypathlib import PathTemplate
from Papers.replicas import PaperReplica
from . import START, STOP
from .refine import W_FILE as R_FILE


W_FILES = {'replica': PathTemplate('$rsrc/pdata/pubmed20n_gz/paper_replicas.pkl'),
           'prints': PathTemplate('$rsrc/pdata/pubmed20n_gz/paper_replicas.txt')}


PaperReplica.R_FILE = R_FILE
PaperReplica.W_FILE = W_FILES['replica']
PaperReplica.START = START
PaperReplica.STOP = STOP
PaperReplica.KEY_ATTR = 'pmid'
main = PaperReplica.main


if __name__ == '__main__':
    main()
