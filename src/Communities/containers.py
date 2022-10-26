from random import choices
from collections.abc import Iterable
from textwrap import dedent, indent
from myclass import MetaCacheExt, MetaDisposal, TarRW
from mypathlib import PathTemplate
from UniProt.containers import Match, KeyWord
from StringMatching.base import unify, tokenize
from Papers.containers import Article
from Patents.containers import Patent


class Community(metaclass=MetaCacheExt):
    CACHE_PATH = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()
    ARTICLE_PATH = PathTemplate('$pdata/paper/sorted/community.tar').substitute()
    PATENT_PATH = PathTemplate('$pdata/patent/sorted/community.tar').substitute()

    def __init__(self, cmnt_idx: int, caching=True):
        self.cmnt_idx: int = cmnt_idx
        self.entries: list[str] = []
        self.keywords: set[str] = set()
        self.pmids: dict[str, set[int]] = {}
        self.pub_numbers: dict[str, set[str]] = {}

    def choices(self, k, *texts):
        if texts:
            pmids = [pmid for text in texts for pmid in self.pmids[text]]
            pubs = [pub for text in texts for pub in self.pub_numbers[text]]
        else:
            pmids = [x for v in self.pmids.values() for x in v]
            pubs = [x for v in self.pub_numbers.values() for x in v]
        return choices(pmids, k=k), choices(pubs, k=k)

    def random_materials(self, k, *texts):
        pmids, pubs = map(set, self.choices(k, *texts))
        arts = [art for art in self.get_articles() if art.pmid in pmids]
        pats = [pat for pat in self.get_patents() if pat.pub_number in pubs]
        return arts, pats

    @property
    def best_known_as(self):
        hs = self.hit_summary()
        if hs:
            return next(iter(hs))
        else:
            return ''

    @property
    def less_info(self):
        res = self.__repr__() + ':  '
        for name in self.hit_summary():
            res += name + ', '
        return res

    @property
    def info(self):
        return dedent(f"""\
        {self}, bka., {self.best_known_as}
               Entries: {_print_set(self.entries)}
              Keywords: {_print_set(self.keywords)}
            Total hits:
                        {self.total_paper_hits:>7,} hits (paper)
                        {self.total_patent_hits:>7,} hits (patent)
        """)

    @property
    def more_info(self):
        return dedent(f"""\
        {self}
               Entries: {_print_set(self.entries)}
              Keywords: {_print_set(self.keywords)}
            Total hits: 
                        {self.total_paper_hits:>7,} hits (paper)
                        {self.total_patent_hits:>7,} hits (patent)
               Details:
        """) + self._more_info() + '\n' if self else self.info

    @property
    def paper_hits(self) -> dict[str, int]:
        """{match.text -> num_hits}"""
        return {k: len(v) for k, v in self.pmids.items()}

    @property
    def total_paper_hits(self) -> int:
        """return total number of paper hits"""
        return sum(self.paper_hits.values())

    def hits(self, text: str) -> tuple[int, int]:
        """return (total_paper_hits, total_patent_hits)"""
        return self.paper_hits.get(text, 0), self.patent_hits.get(text, 0)

    @property
    def total_hits(self) -> int:
        """return total number of hits (regardless of material type)"""
        return self.total_paper_hits + self.total_patent_hits

    def hit_summary(self) -> dict[str, tuple[int, int]]:
        """{match.text -> (total_paper_hits, total_patent_hits)} sorted by paper_hits and patent_hits"""
        keys = sorted(self.paper_hits | self.patent_hits, key=self.hits, reverse=True)
        return {k: self.hits(k) for k in keys}

    def get_articles(self) -> list[Article]:
        if not self.total_paper_hits:
            return []
        with TarRW(self.ARTICLE_PATH, 'r') as tf:
            return tf.gzip_load(f'{self.cmnt_idx}.pkl.gz')  # list[Article]

    @property
    def patent_hits(self) -> dict[str, int]:
        """dict[match.text, num_hits]"""
        return {k: len(v) for k, v in self.pub_numbers.items()}

    @property
    def total_patent_hits(self) -> int:
        return sum(self.patent_hits.values())

    def get_patents(self) -> list[Patent]:
        if not self.total_patent_hits:
            return []
        with TarRW(self.PATENT_PATH, 'r') as tf:
            return tf.gzip_load(f'{self.cmnt_idx}.pkl.gz')  # list[Patent]

    # Fine-details
    def __repr__(self):
        return f"Community({self.cmnt_idx})"

    def __bool__(self):
        return bool(self.total_hits)

    def _more_info(self):
        summary = self.hit_summary()
        form = '{:,}'.format
        ps, ts = zip(*(map(form, x) for x in summary.values()))
        return indent(col_prints(summary, ps, ts, sep=' '), ' '*16)

    # Generation of instances & cache
    def __new__(cls, cmnt_idx: int, caching=True):
        """__new__ method must not be skipped - Assertion of MetaCacheExt"""
        return super().__new__(cls)

    def __getnewargs__(self):
        """__getnewargs__ must not be deleted - Assertion of MetaCacheExt"""
        return self.cmnt_idx, False

    def merge(self, cmnt):
        """Method to merge another Community instance to itself
        This method must not be deleted or renamed - Assertion of MetaCacheExt"""
        for k, v in cmnt.pmids.items():
            self.pmids.setdefault(k, set()).update(v)
        for k, v in cmnt.pub_numbers.items():
            self.pub_numbers.setdefault(k, set()).update(v)

    @classmethod
    def from_parse(cls, cmnt_idx: int, entries: tuple[str], unified_keywords: set[str]):
        new = cls(cmnt_idx)
        new.entries = list(entries)
        new.keywords = UnifyEqKeys.all_equivalents(unified_keywords)

    @classmethod
    def assign(cls, cmnt_idx, attr, key, match):
        """Method where self.pmids & self.pub_numbers are extended"""
        self = cls(cmnt_idx)
        getattr(self, attr).setdefault(match.text, set()).add(key)


class UnifyEqKeys(metaclass=MetaDisposal):
    _R_FILE = PathTemplate('$pdata/uniprot/uniprot_keywords.pkl').substitute()
    DATA = {}

    @classmethod
    def load(cls):
        """.load() or .safe_load() must be called before .all_equivalents()"""
        for keyw in KeyWord.load():  # Read
            match_key = unify(keyw)
            cls.DATA.setdefault(match_key, set()).add(str(keyw))
        KeyFilter.load()

    @classmethod
    def all_equivalents(cls, keywords: Iterable[str], filtered=True):
        """.load() or .safe_load() must be called before .all_equivalents()"""
        # it = (cls.DATA.get(k, {k}) for k in keywords)  # if you want to include old-DB-only keywords
        it = (cls.DATA[k] for k in keywords if k in cls.DATA)  # if you want to exclude old-DB-only keywords
        if filtered:
            it = (x for x in it if all(KeyFilter.isvalid(v) for v in x))
        return set().union(*it)


class KeyFilter(metaclass=MetaDisposal):
    R_FILE = PathTemplate('$data/filtered/filtered_key.txt').substitute()
    DATA = set()

    @classmethod
    def load(cls):
        with open(cls.R_FILE, 'r') as file:  # Read
            lines = (line.partition('#')[0].rstrip() for line in file)
            lines = (line for line in lines if line)
            for line in lines:
                cls.DATA.add(line)

    @classmethod
    def isvalid(cls, k: str):
        return k not in cls.DATA


class TextFilter(metaclass=MetaDisposal):
    R_FILE = PathTemplate('$data/filtered/filtered.txt').substitute()
    DATA = set()

    @classmethod
    def load(cls):
        with open(cls.R_FILE, 'r') as file:  # Read
            lines = (line.partition('#')[0].rstrip() for line in file)
            lines = (line for line in lines if line)
            for line in lines:
                cls.DATA.add(line)

    @classmethod
    def isvalid(cls, x: str):
        if cls.pre_filtered(x):  # TODO Check from WC
            return False
        else:
            return x not in cls.DATA

    @staticmethod
    def pre_filtered(x: str):
        # TODO Check from WC
        return False


class UniKey2Cmnt:
    R_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()

    @classmethod
    def load(cls):
        data = {}
        for cmnt_idx, cmnt in Community.items():
            for key in cmnt.keywords:
                if (uni_key := unify(key)) in data:
                    assert data[uni_key] == cmnt_idx
                else:
                    data[uni_key] = cmnt_idx
        return data


class Manager:
    _R_FILE0 = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()
    _R_FILE1 = PathTemplate('$data/filtered/filtered.txt').substitute()
    _R_FILE2 = PathTemplate('$lite/community/key2cmnt.pkl').substitute()
    _R_FILE3 = PathTemplate('$pdata/uniprot/uniprot_keywords.pkl').substitute()

    def __init__(self):
        Community.import_cache_if_empty(verbose=True)  # Read0
        TextFilter.load()  # Read1
        self.uk2c = UniKey2Cmnt.load()  # Read2
        self.keywords = {k: k for k in KeyWord.load()}  # Read3

    def assign(self, key, attr, *matches):
        already = set()
        for match in matches:
            for cmnt_idx in self.which(match):
                if cmnt_idx not in already:
                    already.add(cmnt_idx)
                    Community.assign(cmnt_idx, attr, key, match)
        return already

    def which(self, match: Match):
        if not TextFilter.isvalid(match.text):
            return  # returns empty generator rather than None, so don't worry
        elif x := self.uk2c.get(unify(match.text)):
            yield x
        else:
            _, ut = tokenize(unify(match.text))
            for key in match.keywords:
                variations = self.keywords[key].get_alts_for_assign()
                if len(ut) == 1 and len(variations) == 1:
                    pass
                elif ut in variations and (y := self.uk2c.get(unify(str(key)))):
                    yield y

    def all_cmnts_in(self, *matches):
        return {cmnt for match in matches for key, cmnt in self.which(match)}


def _print_set(s):
    if len(s) == 1:
        return str(next(iter(s)))
    elif s:
        return sorted(s)
    else:
        return ''


def col_prints(*cols, sep=':'):
    sep += ' '
    ms = [max(len(str(x)) for x in col) for col in cols]
    return '\n'.join((sep.join(f'{x:>{m}}' for x, m in zip(nth, ms))) for nth in zip(*cols))
