import gzip
import json
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP
from .containers import CnPatent
from .parse import parse_cn_patent


_R_FILE = PathTemplate('$data/cn_patent/epoglobal_2023autumn_$number.json.gz')
_W_FILE = PathTemplate('$pdata/cn_patent/epoglobal_2023autumn_$number.pkl.gz')


class Refine:
    R_FILE = PathTemplate('$data/cn_patent/epoglobal_2023autumn_$number.json.gz')
    W_FILE = PathTemplate('$pdata/cn_patent/epoglobal_2023autumn_$number.pkl.gz')
    START = START
    STOP = STOP

    @classmethod
    def read_cn_patents(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number), 'rt', encoding='UTF-8') as file:
            q = [json.loads(line) for line in file]
        return q

    @staticmethod
    def refine_cn_patent(number, x):
        cn_pat = CnPatent.from_parse(*parse_cn_patent(x))
        cn_pat.location = number
        return cn_pat

    @classmethod
    def write(cls, number):
        cn_pats = [cls.refine_cn_patent(number, x) for x in cls.read_cn_patents(number)]
        with gzip.open(cls.W_FILE.substitute(number=number), 'wb') as file:
            pickle.dump(cn_pats, file)
        print(number)

    @classmethod
    def main(cls):
        with Pool(50) as p:
            p.map(cls.write, range(cls.START, cls.STOP))


main = Refine.main


if __name__ == '__main__':
    main()
