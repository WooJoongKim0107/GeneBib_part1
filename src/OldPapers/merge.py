import gzip, pickle
from collections import ChainMap
from more_itertools import pairwise
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP
from .main import W_FILE as R_FILE
from .replicas import PaperReplica


W_FILE = PathTemplate('$rsrc/pdata/pubmed20n_gz/paper_$index.pkl.gz')
replicas = PaperReplica(load=True)


def read_articles(number):
    with gzip.open(R_FILE.substitute(number=number), 'rb') as file:
        return pickle.load(file)


def read_articles_not_repeated(number):
    return {x.pmid: x for x in read_articles(number) if x.pmid not in replicas}


def merge_and_write(index, start, stop):
    papers = iter(read_articles_not_repeated(number) for number in reversed(range(start, stop)))
    res = ChainMap(*papers)
    assert len(res) == sum(len(x) for x in res.maps), f'{index}: ({start}, {stop}) has duplicates'
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)


def append_repeated(index):
    with gzip.open(W_FILE.substitute(index=index)) as file:
        chain = pickle.load(file)
    chain.maps.append(dict(replicas))
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(chain, file)


def main():
    splits = [i for i in range(START, STOP, 10)] + [STOP]
    args = [(index, start, stop) for index, (start, stop) in enumerate(pairwise(splits))]
    with Pool(5) as p:
        p.starmap(merge_and_write, args)
    append_repeated(args[-1][0])


if __name__ == '__main__':
    main()
