import gzip
import json
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP
from .containers import Epo
from Patents.parse import parse_patent


_R_FILE = PathTemplate('$data/epo/epoglobal_2023autumn_$number.json.gz', key='{:0>12}'.format)
_W_FILE = PathTemplate('$pdata/epo/epoglobal_2023autumn_$number.pkl.gz', key='{:0>12}'.format)


class Refine:
    R_FILE = PathTemplate('$data/epo/epoglobal_2023autumn_$number.json.gz', key='{:0>12}'.format)
    W_FILE = PathTemplate('$pdata/epo/epoglobal_2023autumn_$number.pkl.gz', key='{:0>12}'.format)
    START = START
    STOP = STOP

    @classmethod
    def read_epos(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number), 'rt', encoding='UTF-8') as file:
            q = [json.loads(line) for line in file]
        return q

    @staticmethod
    def refine_epo(number, x):
        epo = Epo.from_parse(*parse_patent(x))
        epo.location = number
        return epo

    @classmethod
    def write(cls, number):
        epos = [cls.refine_epo(number, x) for x in cls.read_epos(number)]
        with gzip.open(cls.W_FILE.substitute(number=number), 'wb') as file:
            pickle.dump(epos, file)
        print(number)

    @classmethod
    def main(cls):
        with Pool(50) as p:
            p.map(cls.write, range(cls.START, cls.STOP))


main = Refine.main


if __name__ == '__main__':
    main()
