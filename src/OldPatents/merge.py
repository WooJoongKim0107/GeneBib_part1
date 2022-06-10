import gzip
import pickle
from collections import ChainMap
from more_itertools import pairwise
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP
from .refine import W_FILE as R_FILE
from .replicas import PatentReplica


W_FILE = PathTemplate('$rsrc/pdata/full_raw_200920/patent_$index.pkl.gz')
replicas = PatentReplica(load=True)


def read_patents(number):
    with gzip.open(R_FILE.substitute(number=number), 'rb') as file:
        return pickle.load(file)


def read_patents_not_repeated(number):
    return {x.pub_number: x for x in read_patents(number) if x.pub_number not in replicas}


def merge_and_write(index, start, stop):
    patents = iter(read_patents_not_repeated(number) for number in reversed(range(start, stop)))
    res = ChainMap(*patents)
    assert len(res) == sum(len(x) for x in res.maps), f'{index}: ({start}, {stop}) has duplicates'
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)


def append_repeated(index):
    with gzip.open(W_FILE.substitute(index=index)) as file:
        chain = pickle.load(file)
    chain.maps.append(replicas.selected())
    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(chain, file)


def main():
    splits = [i for i in range(START, STOP, 20)] + [STOP]
    args = [(index, start, stop) for index, (start, stop) in enumerate(pairwise(splits))]
    with Pool(5) as p:
        p.starmap(merge_and_write, args)
    append_repeated(args[-1][0])


if __name__ == '__main__':
    main()
