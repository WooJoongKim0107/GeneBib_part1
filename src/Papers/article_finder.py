import gzip, pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import Journal
from .merge import W_FILE as R_FILE


W_FILES = {'PmidToIdx': PathTemplate('$rsrc/pdata/paper/article_to_index.pkl').substitute(),
           'JnlToPmids': PathTemplate('$rsrc/pdata/paper/journal_to_article$index.$format')}


class PmidToIdx(dict):
    """{pmid -> index}"""
    R_FILE = R_FILE
    W_FILE = W_FILES['PmidToIdx']
    STOP = 112

    def __init__(self, load=True):
        if load:
            data = self.load()
        else:
            data = self.mark_up()
        super().__init__(data)

    @classmethod
    def list_up(cls, index):
        with gzip.open(cls.R_FILE.substitute(index=index), 'rb') as file:
            chain = pickle.load(file)
        res = sorted(art.pmid for art in chain.values())
        print(index)
        return res

    @classmethod
    def mark_up(cls):
        with Pool(5) as p:
            pmids_pack = p.map(cls.list_up, range(cls.STOP))

        location = {}
        for index, pmids in enumerate(pmids_pack):
            for pmid in pmids:
                location[pmid] = index
        return location

    @classmethod
    def load(cls):
        with open(cls.W_FILE, 'rb') as file:
            return pickle.load(file)

    def dump(self):
        with open(self.W_FILE, 'wb') as file:
            pickle.dump(dict(self), file)


class JnlToPmids(dict):
    """{medline_ta -> *pmids}"""
    R_FILE = R_FILE
    W_FILE = W_FILES['JnlToPmids']
    STOP = 112

    def __init__(self, load=True):
        if load:
            data = self.load()
        else:
            data = self.gen()
        super().__init__(data)

    def __getitem__(self, item):
        if isinstance(item, Journal):
            return self.__getitem__(item.medline_ta)
        return super().__getitem__(item)

    @classmethod
    def load(cls):
        with open(cls.W_FILE.substitute(index='', format='pkl'), 'rb') as file:
            return pickle.load(file)

    def dump(self):
        with open(self.W_FILE.substitute(index='', format='pkl'), 'wb') as f:
            pickle.dump(dict(self), f)

    @classmethod
    def gen(cls):
        with Pool(5) as p:
            p.map(cls._write, range(cls.STOP))

        files = [open(cls.W_FILE.substitute(index=i, format='txt'), 'r') for i in range(cls.STOP)]

        res = {}
        for j, v in Journal.items():
            iarts = iter(cls._revert(file.readline()[:-1]) for file in files)
            arts = sorted(sum(iarts, []))
            res[j] = arts
            v.counts = len(arts)

        for file in files:
            file.close()
        for i in range(cls.STOP):
            cls.W_FILE.substitute(index=i, format='txt').unlink()
        return res

    @classmethod
    def _write(cls, index):
        with gzip.open(cls.R_FILE.substitute(index=index), 'rb') as file:
            chain = pickle.load(file)

        j2a = {}
        for k, v in Journal.items():
            j2a.setdefault(k, j2a.setdefault(v.medline_ta, []))
        for art in chain.values():
            j2a[art._journal_title].append(art.pmid)
        for arts in j2a.values():
            arts.sort()

        with open(cls.W_FILE.substitute(index=index, format='txt'), 'w') as file:
            for j in Journal:
                file.write(cls._convert(j, j2a.get(j, [])))
                file.write('\n')
        print(index)

    @staticmethod
    def _convert(j, arts):
        return j + '$:$' + ','.join(map(str, arts))

    @staticmethod
    def _revert(s):
        j0, x = s.split('$:$')
        if x:
            arts = list(map(int, x.split(',')))
        else:
            arts = []
        return arts


class ArticleFinder:
    R_FILE = R_FILE
    J2A = None

    @classmethod
    def from_pmids(cls, *pmids):
        for idx, pmids in cls._quick_recipe(*pmids).items():
            with gzip.open(cls.R_FILE.substitute(index=idx), 'rb') as file:
                chain = pickle.load(file)
            for pmid in pmids:
                yield chain[pmid]

    @classmethod
    def from_journal(cls, j):
        if cls.J2A is None:
            cls.J2A = JnlToPmids(load=True)
        return cls.from_pmids(*cls.J2A[j])

    @staticmethod
    def _quick_recipe(*pmids):
        pmid2idx = PmidToIdx()
        idx2pmids = {}
        for pmid in pmids:
            idx = pmid2idx[pmid]
            idx2pmids.setdefault(idx, []).append(pmid)
        return idx2pmids


def main():
    PmidToIdx(load=False).dump()
    JnlToPmids(load=False).dump()
    Journal.export_cache()


if __name__ == '__main__':
    main()
