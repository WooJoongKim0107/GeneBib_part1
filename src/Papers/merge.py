import gzip
import pickle
from collections import ChainMap
from more_itertools import pairwise
from multiprocessing import Pool
from mypathlib import PathTemplate
from .main import W_FILE as R_FILE
from .replicas import Replica


W_FILE = PathTemplate('$rsrc/pdata/paper/paper_$index.pkl.gz')
replicas = Replica(load=True)


def read_articles(number):
    with gzip.open(R_FILE.substitute(number=number), 'rb') as file:
        return pickle.load(file)


def read_articles_not_repeated(number):
    return {x.pmid: x for x in read_articles(number) if x not in replicas}


def merge_and_write(index, start, stop):
    papers = iter(read_articles_not_repeated(number) for number in reversed(range(start, stop)))
    res = ChainMap(*papers)
    assert len(res) == sum(len(x) for x in res.maps)
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)


def append_repeated(index):
    with open(W_FILE.substitute(index=index)) as file:
        chain = pickle.load(file)
    chain.maps.append(dict(replicas))
    with open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(chain, file)


def main():
    splits = [i for i in range(1, 1115, 10)] + [1115]
    args = [(index, start, stop) for index, (start, stop) in enumerate(pairwise(splits))]
    with Pool(5) as p:
        p.starmap(merge_and_write, args)
    append_repeated(len(args)-1)


if __name__ == '__main__':
    main()
