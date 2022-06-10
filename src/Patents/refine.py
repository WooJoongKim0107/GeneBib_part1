import gzip
import json
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP
from .containers import Patent
from .parse import parse_patent

R_FILE = PathTemplate('$rsrc/data/patent/patent_202111_$number.json.gz', key='{:0>12}'.format)
W_FILE = PathTemplate('$rsrc/pdata/patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)


class Refine:
    R_FILE = R_FILE
    W_FILE = W_FILE
    START = START
    STOP = STOP

    @classmethod
    def select_US_patents(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number), 'rt', encoding='UTF-8') as file:
            q = [json.loads(line) for line in file]
        return [x for x in q if x['country_code'] == 'US']

    @staticmethod
    def refine_patent(number, x):
        patent = Patent.from_parse(*parse_patent(x))
        patent.location = number
        return patent

    @classmethod
    def write(cls, number):
        patents = [cls.refine_patent(number, x) for x in cls.select_US_patents(number)]
        with gzip.open(cls.W_FILE.substitute(number=number), 'wb') as file:
            pickle.dump(patents, file)
        print(number)

    @classmethod
    def main(cls):
        with Pool(6) as p:
            p.map(cls.write, range(cls.START, cls.STOP))


main = Refine.main


if __name__ == '__main__':
    main()
