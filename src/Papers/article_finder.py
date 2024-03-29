from multiprocessing import Pool
from myclass import MetaLoad
from mypathlib import PathTemplate
from Papers import Article, Journal  # RW(R)


_R_FILE = PathTemplate('$pdata/paper/paper_$index.pkl.gz')
_RW_FILES = {'PmidToIdx': PathTemplate('$lite/paper/article_to_index.pkl').substitute(),
             'JnlToPmids': PathTemplate('$lite/paper/journal_to_article.pkl').substitute(),
             '': PathTemplate('$lite/paper/journal_to_article$index.txt')}
_RW_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()


class PmidToIdx(dict, metaclass=MetaLoad):
    """{pmid -> index}"""
    _R_FILE = PathTemplate('$pdata/paper/paper_$index.pkl.gz')
    LOAD_PATH = PathTemplate('$lite/paper/article_to_index.pkl').substitute()
    STOP = 112

    @classmethod
    def list_up(cls, index):
        res = sorted(art.pmid for art in Article.load(index).values())  # Read
        print(index)
        return res

    @classmethod
    def generate(cls):
        with Pool(50) as p:
            pmids_pack = p.map(cls.list_up, range(cls.STOP))

        for index, pmids in enumerate(pmids_pack):
            for pmid in pmids:
                yield pmid, index


class JnlToPmids(dict, metaclass=MetaLoad):
    """{medline_ta -> *pmids}"""
    _R_FILE = PathTemplate('$pdata/paper/paper_$index.pkl.gz')
    LOAD_PATH = PathTemplate('$lite/paper/journal_to_article.pkl').substitute()
    RW_FILE = PathTemplate('$lite/paper/journal_to_article$index.txt')
    STOP = 112

    def __getitem__(self, item):
        if isinstance(item, Journal):
            return self.__getitem__(item.medline_ta)
        return super().__getitem__(item)

    @classmethod
    def generate(cls):
        with Pool(50) as p:
            p.map(cls._write, range(cls.STOP))  # RW(W)

        files = [open(cls.RW_FILE.substitute(index=i), 'r') for i in range(cls.STOP)]  # RW(R)

        res = {}
        for j, v in Journal.items():
            iarts = iter(cls._revert(file.readline()[:-1]) for file in files)
            arts = sorted(sum(iarts, []))
            res[j] = arts
            v.counts = len(arts)

        for file in files:
            file.close()
        for i in range(cls.STOP):
            cls.RW_FILE.substitute(index=i).unlink()
        return res

    @classmethod
    def _write(cls, index):
        j2a = {}
        for k, v in Journal.items():
            j2a.setdefault(k, j2a.setdefault(v.medline_ta, []))
        for art in Article.load(index).values():  # Read
            j2a[art._journal_title].append(art.pmid)
        for arts in j2a.values():
            arts.sort()

        with open(cls.RW_FILE.substitute(index=index), 'w') as file:  # RW(W)
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
    _R_FILE = PathTemplate('$pdata/paper/paper_$index.pkl.gz')
    _R_FILE0 = PathTemplate('$lite/paper/journal_to_article.pkl').substitute()
    _R_FILE1 = PathTemplate('$lite/paper/article_to_index.pkl').substitute()
    J2A = None

    @classmethod
    def from_pmids(cls, *pmids):
        for idx, pmids in cls._quick_recipe(*pmids).items():
            chain = Article.load(idx)  # Read
            arts = [chain[pmid] for pmid in pmids]
            del chain

            for art in arts:
                yield art

    @classmethod
    def from_journal(cls, j):
        if cls.J2A is None:
            cls.J2A = JnlToPmids.load()  # Read0
        return cls.from_pmids(*cls.J2A[j])

    @staticmethod
    def _quick_recipe(*pmids):
        pmid2idx = PmidToIdx.load()  # Read1
        idx2pmids = {}
        for pmid in pmids:
            idx = pmid2idx[pmid]
            idx2pmids.setdefault(idx, []).append(pmid)
        return idx2pmids


def main():
    PmidToIdx.build()  # RW['PmidToIdx'](W)
    JnlToPmids.build()  # RW['JnlToPmids'](W)
    Journal.export_cache()  # RW(W)


if __name__ == '__main__':
    main()
