from mypathlib import PathTemplate
from Papers.replicas import PaperReplica
from . import START, STOP
from .main import W_FILE as R_FILE


W_FILES = {'replica': PathTemplate('$rsrc/pdata/pubmed20n_gz/paper_replicas.pkl'),
           'prints': PathTemplate('$rsrc/pdata/pubmed20n_gz/paper_replicas.txt')}


PaperReplica.R_FILE = R_FILE
PaperReplica.W_FILE = W_FILES['replica']
PaperReplica.START = START
PaperReplica.STOP = STOP
PaperReplica.KEY_ATTR = 'pmid'


if __name__ == '__main__':
    q = PaperReplica(load=False)
    q.dump()
    with open(W_FILES['prints'].substitute(), 'w', encoding='UTF-8') as file:
        file.write(q.full_comparison())
