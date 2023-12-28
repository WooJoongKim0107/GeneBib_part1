import gzip
import json
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP
from .containers import EpPatent
from .parse import parse_ep_patent


_R_FILE = PathTemplate('$data/ep_patent/epoglobal_2023autumn_$number.json.gz')
_W_FILE = PathTemplate('$pdata/ep_patent/epoglobal_2023autumn_$number.pkl.gz')


class Refine:
    R_FILE = PathTemplate('$data/ep_patent/epoglobal_2023autumn_$number.json.gz')
    W_FILE = PathTemplate('$pdata/ep_patent/epoglobal_2023autumn_$number.pkl.gz')
    START = START
    STOP = STOP

    @classmethod
    def read_ep_patents(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number), 'rt', encoding='UTF-8') as file:
            q = [json.loads(line) for line in file]
        return q

    @staticmethod
    def refine_ep_patent(number, x):
        ep_pat = EpPatent.from_parse(*parse_ep_patent(x))
        ep_pat.location = number
        return ep_pat

    @classmethod
    def write(cls, number):
        ep_pats = [cls.refine_ep_patent(number, x) for x in cls.read_ep_patents(number)]
        with gzip.open(cls.W_FILE.substitute(number=number), 'wb') as file:
            pickle.dump(ep_pats, file)
        print(number)

    @classmethod
    def main(cls):
        with Pool(50) as p:
            p.map(cls.write, range(cls.START, cls.STOP))


main = Refine.main


if __name__ == '__main__':
    main()
