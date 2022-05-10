import gzip
import pickle
from more_itertools import pairwise
from mypathlib import PathTemplate
from .main import W_FILE as R_FILE


W_FILE = PathTemplate('$rsrc/pdata/paper/article_$index.pkl')


def read_paper(number):
    with gzip.open(R_FILE.substitute(number=number), 'rb') as file:
        return pickle.load(file)


def merge_paper(index, start, stop):
    res = [read_paper(number) for number in range(start, stop)]
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)


def main_paper():
    splits = [i for i in range(1, 1115, 40)] + [1115]
    for index, (start, stop) in enumerate(pairwise(splits)):
        merge_paper(index, start, stop)


if __name__ == '__main__':
    main_paper()
