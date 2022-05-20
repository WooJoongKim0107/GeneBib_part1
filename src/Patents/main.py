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


def get_patent(number, x):
    patent = Patent.from_parse(*parse_patent(x))
    patent.location = number
    return patent


def select_US_patents(number):
    print(number)
    with gzip.open(R_FILE.substitute(number=number), 'rt', encoding='UTF-8') as file:
        q = [json.loads(line) for line in file]
    return [x for x in q if x['country_code'] == 'US']


def write(number):
    patents = [get_patent(number, x) for x in select_US_patents(number)]
    with gzip.open(W_FILE.substitute(number=number), 'wb') as file:
        pickle.dump(patents, file)


def main():
    with Pool() as p:
        p.map(write, range(START, STOP))


if __name__ == '__main__':
    main()
