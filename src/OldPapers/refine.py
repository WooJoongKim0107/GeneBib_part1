from mypathlib import PathTemplate
from Papers.refine import Refine
from . import START, STOP
from .containers import Journal


R_FILE = PathTemplate('$rsrc/data/pubmed20n_gz/pubmed20n$number.xml.gz', key='{:0>4}'.format)
W_FILE = PathTemplate('$rsrc/pdata/pubmed20n_gz/article20n$number.pkl.gz', key='{:0>4}'.format)
MSG = PathTemplate('$rsrc/pdata/pubmed20n_gz/article20n$number.txt', key='{:0>4}'.format)


Refine.R_FILE = R_FILE
Refine.W_FILE = W_FILE
Refine.MSG = MSG
Refine.START = START
Refine.STOP = STOP
Refine.JNL = Journal
main = Refine.main


if __name__ == '__main__':
    main()
