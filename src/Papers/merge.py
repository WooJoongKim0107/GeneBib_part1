import gzip
import pickle
from collections import ChainMap
from more_itertools import pairwise
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP
from .replicas import PaperReplica


R_FILE = PathTemplate('$pdata/paper/article22n$number.pkl.gz', key='{:0>4}'.format)
_R_FILE0 = PathTemplate('$pdata/paper/paper_replicas.pkl')
W_FILE = PathTemplate('$pdata/paper/paper_$index.pkl.gz')


class Merge:
    R_FILE = PathTemplate('')
    W_FILE = PathTemplate('')
    REPLICAS = PaperReplica()
    KEY_ATTR = ''
    START = 0
    STOP = 0
    STEP = 0

    @classmethod
    def assign_constants(cls, r_file, w_file, replicas, key_attr, start, stop, step):
        cls.R_FILE = r_file
        cls.W_FILE = w_file
        cls.REPLICAS = replicas
        cls.KEY_ATTR = key_attr
        cls.START = start
        cls.STOP = stop
        cls.STEP = step

    @classmethod
    def read(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number), 'rb') as file:
            return pickle.load(file)

    @classmethod
    def read_not_repeated(cls, number):
        return {getattr(x, cls.KEY_ATTR): x for x in cls.read(number) if getattr(x, cls.KEY_ATTR) not in cls.REPLICAS}

    @classmethod
    def merge_and_write(cls, index, start, stop):
        papers = iter(cls.read_not_repeated(number) for number in reversed(range(start, stop)))
        res = ChainMap(*papers)
        assert len(res) == sum(len(x) for x in res.maps), f'{index}: ({start}, {stop}) has duplicates'
        with gzip.open(cls.W_FILE.substitute(index=index), 'wb') as file:
            pickle.dump(res, file)
        print(index)

    @classmethod
    def append_repeated(cls, index):
        with gzip.open(cls.W_FILE.substitute(index=index)) as file:
            chain = pickle.load(file)
        chain.maps.append(cls.REPLICAS.selected())
        with gzip.open(cls.W_FILE.substitute(index=index), 'wb') as file:
            pickle.dump(chain, file)

    @classmethod
    def main(cls):
        cls.REPLICAS = type(cls.REPLICAS).load()  # Read0
        splits = [i for i in range(cls.START, cls.STOP, cls.STEP)] + [cls.STOP]
        args = [(index, start, stop) for index, (start, stop) in enumerate(pairwise(splits))]
        with Pool(50) as p:
            p.starmap(cls.merge_and_write, args)
        cls.append_repeated(args[-1][0])


Merge.assign_constants(R_FILE, W_FILE, PaperReplica(), 'pmid', START, STOP, 10)
main = Merge.main

if __name__ == '__main__':
    main()
