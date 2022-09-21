from mypathlib import PathTemplate
from Papers.pioneer import Pioneer
from . import START, STOP
from .containers import Journal


R_FILE = PathTemplate('$rsrc/data/pubmed20n_gz/pubmed20n$number.xml.gz', key='{:0>4}'.format)
_W_FILE = PathTemplate('$rsrc/pdata/pubmed20n_gz/journal_cache.pkl.gz')
assert _W_FILE.substitute() == Journal.CACHE_PATH


Pioneer.R_FILE = R_FILE
Pioneer._W_FILE = _W_FILE
Pioneer.START = START
Pioneer.STOP = STOP
Pioneer.JNL = Journal
main = Pioneer.main


if __name__ == '__main__':
    main()
