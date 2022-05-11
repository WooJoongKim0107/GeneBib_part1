import gzip
import pickle
from collections import ChainMap
from more_itertools import pairwise
from multiprocessing import Pool
from mypathlib import PathTemplate
from .main import W_FILE as R_FILE


W_FILE = PathTemplate('$rsrc/pdata/patent/patent_$index.pkl.gz')


def read_patent(number):
    with gzip.open(R_FILE.substitute(number=number), 'rb') as file:
        return pickle.load(file)


def merge_patent(index, start, stop):
    patents = iter(read_patent(number) for number in reversed(range(start, stop)))
    res = ChainMap(*patents)
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)


def main_patent():
    splits = [i for i in range(0, 10033, 90)] + [10033]
    args = [(index, start, stop) for index, (start, stop) in enumerate(pairwise(splits))]
    with Pool(5) as p:
        p.starmap(merge_patent, args)


if __name__ == '__main__':
    main_patent()
