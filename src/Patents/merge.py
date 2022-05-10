import gzip
import pickle
from more_itertools import pairwise
from mypathlib import PathTemplate
from Patents.main import W_FILE as R_FILE


W_FILE = PathTemplate('$rsrc/pdata/patent/patent_$index.pkl')


def read_patent(number):
    with gzip.open(R_FILE.substitute(number=number), 'rb') as file:
        return pickle.load(file)


def merge_patent(index, start, stop):
    res = [read_patent(number) for number in range(start, stop)]
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)


def main_patent():
    splits = [1, 223, 445, 667, 889, 1115]
    for index, (start, stop) in enumerate(pairwise(splits)):
        merge_patent(index, start, stop)


if __name__ == '__main__':
    main_patent()
