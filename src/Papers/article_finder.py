import gzip, pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import Journal
from .merge import W_FILE as R_FILE


W_FILES = {'PmidToIdx': PathTemplate('$rsrc/pdata/paper/article_to_index.pkl').substitute(),
           'JnlToArt': PathTemplate('$rsrc/pdata/paper/journal_to_article$index.txt')}


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


class JnlToArt:
    """{medline_ta -> pmid}"""
    R_FILE = R_FILE
    W_FILE = W_FILES['JnlToArt']
    STOP = 112

    @classmethod
    def find_arts(cls, j):
        pmid2idx = PmidToIdx()
        idx2pmids = {}
        for pmid in cls.find_pmids(j):
            idx = pmid2idx[pmid]
            idx2pmids.setdefault(idx, []).append(pmid)

        arts = []
        for idx, pmids in idx2pmids.items():
            with gzip.open(cls.R_FILE.substitute(index=idx), 'rb') as file:
                chain = pickle.load(file)
            for pmid in pmids:
                arts.append(chain[pmid])
        return arts

    @classmethod
    def find_pmids(cls, j):
        if isinstance(j, str):
            key = j
            n = cls._find_n(key)
        elif isinstance(j, Journal):
            key = j.medline_ta
            n = cls._find_n(key)
        elif isinstance(j, int):
            n = j
        else:
            raise ValueError(f'Cannot recognize type({type(j)})')
        return cls._read_n(n)

    @classmethod
    def write(cls):
        with Pool(5) as p:
            p.map(cls._write, range(cls.STOP))

        with open(cls.W_FILE.substitute(index=''), 'w') as f:
            files = [open(cls.W_FILE.substitute(index=i), 'r') for i in range(cls.STOP)]
            for j in Journal:
                iarts = iter(cls.revert(file.readline()[:-1]) for file in files)
                arts = sorted(sum(iarts, []))
                f.write(cls.convert(j, arts))
                f.write('\n')
            for file in files:
                file.close()

        for i in range(cls.STOP):
            cls.W_FILE.substitute(index=i).unlink()

    @classmethod
    def _write(cls, index):
        with gzip.open(cls.R_FILE.substitute(index=index), 'rb') as file:
            chain = pickle.load(file)

        j2a = {}
        for art in chain.values():
            j2a.setdefault(art._journal_title, []).append(art.pmid)

        for arts in j2a.values():
            arts.sort()

        with open(cls.W_FILE.substitute(index), 'w') as file:
            for j in Journal:
                file.write(cls.convert(j, j2a.get(j, [])))
                file.write('\n')
        print(index)

    @staticmethod
    def _find_n(key):
        for i, j in enumerate(Journal):
            if j == key:
                return i

    @classmethod
    def _read_n(cls, n):
        with open(cls.W_FILE.substitute(index=''), 'r') as f:
            for i in range(n-1):
                f.readline()
            s = f.readline()
        return cls.revert(s)

    @staticmethod
    def convert(j, arts):
        return j + '$:$' + ','.join(map(str, arts))

    @staticmethod
    def revert(s):
        j0, x = s.split('$:$')
        if x:
            arts = list(map(int, x.split(',')))
        else:
            arts = []
        return arts


class ArticleFinder:
    R_FILE = R_FILE

    @classmethod
    def from_pmid(cls, *pmids):
        pmid2idx = PmidToIdx()
        idx2pmids = {}
        for pmid in pmids:
            idx = pmid2idx[pmid]
            idx2pmids.setdefault(idx, []).append(pmid)

        arts = []
        for idx, pmids in idx2pmids.items():
            with gzip.open(cls.R_FILE.substitute(index=idx), 'rb') as file:
                chain = pickle.load(file)
            for pmid in pmids:
                arts.append(chain[pmid])

        if len(pmids) == 1:
            return arts[0]
        else:
            return arts

    @classmethod
    def from_journal(cls, *js):
        if len(js) == 1:
            return JnlToArt.find_arts(js[0])
        else:
            return {j: JnlToArt.find_arts(j) for j in js}


def main():
    PmidToIdx(False).dump()
    JnlToArt.write()
