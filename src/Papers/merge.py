import gzip
import pickle
from collections import ChainMap
from more_itertools import pairwise
from multiprocessing import Pool
from mypathlib import PathTemplate
from .main import W_FILE as R_FILE


W_FILE = PathTemplate('$rsrc/pdata/paper/paper_$index.pkl.gz')


def read_paper(number):
    with gzip.open(R_FILE.substitute(number=number), 'rb') as file:
        return pickle.load(file)


def merge_paper(index, start, stop):
    papers = iter(read_paper(number) for number in reversed(range(start, stop)))
    res = ChainMap(*papers)
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)


def main_paper():
    splits = [i for i in range(1, 1115, 10)] + [1115]
    args = [(index, start, stop) for index, (start, stop) in enumerate(pairwise(splits))]
    with Pool(5) as p:
        p.starmap(merge_paper, args)


if __name__ == '__main__':
    main_paper()
