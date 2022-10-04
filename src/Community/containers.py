import gzip
import pickle
from collections.abc import Iterable
from myclass import MetaCacheExt, MetaLoad, MetaDisposal
from mypathlib import PathTemplate
from UniProt.containers import Match
from StringMatching.base import unify


class Community(metaclass=MetaCacheExt):
    R_FILE = PathTemplate('$rsrc/pdata/uniprot/uniprot_sprot_parsed.pkl.gz').substitute()
    CACHE_PATH = PathTemplate('$rsrc/pdata/community/community_cache.pkl.gz').substitute()
    ENTRIES = {}

    def __new__(cls, cmnt_idx: int, caching=True):
        """__new__ method must not be skipped - Assertion of MetaCacheExt"""
        return super().__new__(cls)

    def __init__(self, cmnt_idx: int, caching=True):
        self.cmnt_idx: int = cmnt_idx
        self.entries: list[str] = []
        self.keywords: set[str] = set()
        self.pmids: dict[str, set[int]] = {}
        self.pub_numbers: dict[str, set[str]] = {}

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
    def load_entries(cls):
        with gzip.open(cls.R_FILE, 'rb') as file:  # Read
            cls.ENTRIES = pickle.load(file)

    """
    def get_keywords(self):
        entries = [self.ENTRIES[ent] for ent in self.entries if ent in self.ENTRIES]
        keywords = {k for ent in entries for k in ent.keywords}
        tobe_added = set().union(*(ent.added_keywords for ent in entries))
        tobe_removed = set().union(*(ent.removed_keywords for ent in entries))
        return keywords | tobe_added - tobe_removed
    """


class Key2Cmnt(dict, metaclass=MetaLoad):
    _R_FILE0 = PathTemplate('$rsrc/pdata/community/community_cache.pkl.gz').substitute()
    _R_FILE1 = PathTemplate('$rsrc/pdata/uniprot/uniprot_sprot_parsed.pkl.gz').substitute()
    LOAD_PATH = PathTemplate('$rsrc/lite/community/key2cmnt.pkl').substitute()

    @classmethod
    def generate(cls):
        Community.import_cache_if_empty(verbose=True)  # Read0
        Community.load_entries()  # Read1
        for cmnt in Community.values():
            for key in cmnt.keywords:
                yield key, cmnt.cmnt_idx


class UnifyEqKeys(metaclass=MetaDisposal):
    R_FILE = PathTemplate('$rsrc/pdata/uniprot/uniprot_keywords.pkl').substitute()
    DATA = {}

    @classmethod
    def load(cls):
        """.load() or .safe_load() must be called before .all_equivalents()"""
        with open(cls.R_FILE, 'rb') as file:  # Read
            for keyw in pickle.load(file):
                match_key = unify(keyw)
                cls.DATA.setdefault(match_key, set()).add(str(keyw))

    @classmethod
    def all_equivalents(cls, keywords: Iterable[str]):
        """.load() or .safe_load() must be called before .all_equivalents()"""
        it = (cls.DATA.get(k, k) for k in keywords)  # TODO run UniProt & StringMatching again with modification
        return set().union(*it)


class TextFilter(metaclass=MetaDisposal):
    R_FILE = PathTemplate('$rsrc/data/filtered/filtered.txt').substitute()
    DATA = set()

    @classmethod
    def load(cls):
        with open(cls.R_FILE, 'r') as file:  # Read
            for x in file.read().splitlines():
                cls.DATA.add(x)

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


class Manager:
    _R_FILE0 = PathTemplate('$rsrc/pdata/community/community_cache.pkl.gz').substitute()
    _R_FILE1 = PathTemplate('$rsrc/data/filtered/filtered.txt').substitute()
    _R_FILE2 = PathTemplate('$rsrc/lite/community/key2cmnt.pkl').substitute()

    def __init__(self):
        Community.import_cache_if_empty(verbose=True)  # Read0
        TextFilter.load()  # Read1
        self.k2c = Key2Cmnt.load()  # Read2

    def assign(self, key, attr, *matches):
        already = set()
        for match in matches:
            for keyword, cmnt_idx in self.which(match):
                if cmnt_idx not in already:
                    already.add(cmnt_idx)
                    getattr(Community[cmnt_idx], attr).setdefault(keyword, set()).add(key)
        return already

    def which(self, match: Match):
        if TextFilter.isvalid(match.text):
            for keyword in match.keywords:
                if keyword in self.k2c:
                    yield keyword, self.k2c[keyword]

    def all_cmnts_in(self, *matches):
        return {cmnt for match in matches for key, cmnt in self.which(match)}
