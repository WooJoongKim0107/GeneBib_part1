from multiprocessing import Pool
from myclass import MetaLoad
from mypathlib import PathTemplate
from UsPatents import UsPatent


_R_FILE = PathTemplate('$pdata/us_patent/patent_index.pkl.gz')
_W_FILES = {'PmidToIdx': PathTemplate('$lite/us_patent/pubnum_to_index.pkl').substitute()}


class PubToIdx(dict, metaclass=MetaLoad):
    """{pub_number -> index}"""
    _R_FILE = PathTemplate('$pdata/paper/patent_$index.pkl.gz')
    LOAD_PATH = PathTemplate('$lite/paper/pubnum_to_index.pkl').substitute()
    STOP = 112

    @classmethod
    def list_up(cls, index):
        res = sorted(art.pmid for art in UsPatent.load(index).values())
        print(index)
        return res

    @classmethod
    def generate(cls):
        with Pool(50) as p:
            pmids_pack = p.map(cls.list_up, range(cls.STOP))

        for index, pmids in enumerate(pmids_pack):
            for pmid in pmids:
                yield pmid, index


class PatentFinder:
    _R_FILE = PathTemplate('$pdata/us_patent/patent_$index.pkl.gz')
    _R_FILE1 = PathTemplate('$lite/us_patent/pubnum_to_index.pkl').substitute()

    @classmethod
    def from_pmids(cls, *pub_nums):
        for idx, pub_nums in cls._quick_recipe(*pub_nums).items():
            chain = UsPatent.load(idx)
            patents = [chain[pub_num] for pub_num in pub_nums]
            del chain

            for patent in patents:
                yield patent

    @staticmethod
    def _quick_recipe(*pmids):
        pmid2idx = PubToIdx.load()  # Read1
        idx2pmids = {}
        for pmid in pmids:
            idx = pmid2idx[pmid]
            idx2pmids.setdefault(idx, []).append(pmid)
        return idx2pmids


main = PubToIdx.build()


if __name__ == '__main__':
    main()
