from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers.merge import Merge
from . import STARTS, STOPS
from .replicas import EpoReplica


R_FILE = PathTemplate('$pdata/epo/epo_202111_$number.pkl.gz', key='{:0>12}'.format)
_R_FILE = PathTemplate('$pdata/epo/epo_replicas.pkl')
W_FILE = PathTemplate('$pdata/epo/epo_$index.pkl.gz')


class Merge2(Merge):
    @classmethod
    def main(cls):
        cls.REPLICAS = type(cls.REPLICAS).load()  # Read0
        args = [(index, start, stop) for index, (start, stop) in enumerate(zip(STARTS, STOPS))]
        with Pool(10) as p:
            p.starmap(cls.merge_and_write, args)
        cls.append_repeated(args[-1][0])


Merge2.assign_constants(R_FILE, W_FILE, EpoReplica.load(), 'epo_number', 0, 0, 0)  # Read
main = Merge2.main


if __name__ == '__main__':
    main()
