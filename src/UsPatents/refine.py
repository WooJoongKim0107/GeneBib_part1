import gzip
import json
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP
from .containers import UsPatent
from .parse import parse_us_patent


_R_FILE = PathTemplate('$data/us_patent/patent_202111_$number.json.gz', key='{:0>12}'.format)  # TODO Jan 23rd
_W_FILE = PathTemplate('$pdata/us_patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)  # TODO Jan 23rd


class Refine:
    R_FILE = PathTemplate('$data/us_patent/patent_202111_$number.json.gz', key='{:0>12}'.format)  # TODO Jan 23rd
    W_FILE = PathTemplate('$pdata/us_patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)  # TODO Jan 23rd
    START = START
    STOP = STOP

    @classmethod
    def read_us_patents(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number), 'rt', encoding='UTF-8') as file:
            q = [json.loads(line) for line in file]
        return q

    @staticmethod
    def refine_us_patent(number, x):
        us_pat = UsPatent.from_parse(*parse_us_patent(x))
        us_pat.location = number
        return us_pat

    @classmethod
    def write(cls, number):
        us_pats = [cls.refine_us_patent(number, x) for x in cls.read_us_patents(number)]
        with gzip.open(cls.W_FILE.substitute(number=number), 'wb') as file:
            pickle.dump(us_pats, file)
        print(number)

    @classmethod
    def main(cls):
        with Pool(50) as p:
            p.map(cls.write, range(cls.START, cls.STOP))


main = Refine.main


if __name__ == '__main__':
    main()
