from multiprocessing import Pool
from mypathlib import PathTemplate
from merge import Merge
from . import STARTS, STOPS
from .replicas import CnPatentReplica


R_FILE = PathTemplate('$pdata/cn_patent/epoglobal_2023autumn_$number.pkl.gz')
_R_FILE = PathTemplate('$pdata/cn_patent/patent_replicas.pkl')
W_FILE = PathTemplate('$pdata/cn_patent/patent_$index.pkl.gz')


class Merge2(Merge):
    @classmethod
    def main(cls):
        cls.REPLICAS = type(cls.REPLICAS).load()  # Read0
        args = [(index, start, stop) for index, (start, stop) in enumerate(zip(STARTS, STOPS))]
        with Pool(10) as p:
            p.starmap(cls.merge_and_write, args)
        cls.append_repeated(args[-1][0])


Merge2.assign_constants(R_FILE, W_FILE, CnPatentReplica.load(), 'pub_number', 0, 0, 0)  # Read
main = Merge2.main


if __name__ == '__main__':
    main()
