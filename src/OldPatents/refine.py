from mypathlib import PathTemplate
from . import START, STOP
from Patents.refine import Refine


R_FILE = PathTemplate('$rsrc/data/full_raw_200920/patent_200920_$number.json.gz', key='{:0>12}'.format)
W_FILE = PathTemplate('$rsrc/pdata/full_raw_200920/patent_200920_$number.pkl.gz', key='{:0>12}'.format)


Refine.R_FILE = R_FILE
Refine.W_FILE = W_FILE
Refine.START = START
Refine.STOP = STOP
main = Refine.main


if __name__ == '__main__':
    main()
