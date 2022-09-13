import gzip
import pickle
from UniProt.containers import UniProtEntry
from myclass.cached_class import MetaCacheExt
from mypathlib import PathTemplate


class Community(metaclass=MetaCacheExt):
    ENTRIES = {}
    R_FILE = PathTemplate('$rsrc/pdata/uniprot/uniprot_sprot_parsed.pkl.gz').substitute()
    _CACHE_PATH = PathTemplate('$rsrc/pdata/community/community_cache.pkl.gz').substitute()

    def __new__(cls, cmnt_idx: int, caching=True):
        """__new__ method must not be skipped - Assertion of MetaCacheExt"""
        return super().__new__(cls)

    def __init__(self, cmnt_idx: int, caching=True):
        self.cmnt_idx: int = cmnt_idx
        self.entries: list[str] = []
        self.added_keywords: set[str] = set()
        self.removed_keywords: set[str] = set()
        self.pmids: list[int] = []

    def __getnewargs__(self):
        """__getnewargs__ must not be deleted - Assertion of MetaCacheExt"""
        return self.cmnt_idx, False

    def merge(self, cmnt):
        """Method to merge another Community instance to itself
        This method must not be deleted or renamed - Assertion of MetaCacheExt"""
        self.pmids.extend(cmnt.pmids)

    @classmethod
    def from_parse(cls, cmnt_idx: int, entries: list[str], added: list[str], removed: list[str]):
        new = cls(cmnt_idx)
        new.entries = [UniProtEntry(ent) for ent in entries]
        new.added = added
        new.removed = removed

    @classmethod
    def load_entries(cls):
        with gzip.open(cls.R_FILE, 'rb') as file:
            cls.ENTRIES = pickle.load(file)

    def get_keywords(self, entries_dict=None):
        entries_dict = self.ENTRIES if entries_dict is None else entries_dict
        entries = [entries_dict[ent] for ent in self.entries]
        keywords = {k for ent in entries for k in ent.keywords}
        tobe_added = set().union(*(ent.added_keywords for ent in entries))
        tobe_removed = set().union(*(ent.removed_keywords for ent in entries))
        return keywords | tobe_added - tobe_removed


# if Community._CACHE_PATH.is_file():
#     Community.import_cache(verbose=True)


class Ent2Cmnt(dict):
    RW_FILE = PathTemplate('$rsrc/lite/community/ent2cmnt.pkl').substitute()

    def __init__(self, load=True):
        if load:
            data = self.load()
        else:
            data = self.generate()
        super().__init__(data)

    @classmethod
    def load(cls):
        with open(cls.RW_FILE, 'rb') as file:
            return pickle.load(file)

    @staticmethod
    def generate():
        Community.import_cache_if_empty(verbose=True)
        for cmnt_idx, cmnt in Community.items():
            for ent in cmnt.entries:
                yield ent, cmnt_idx


class Key2Cmnt(dict):
    _R_FILE = PathTemplate('$rsrc/pdata/uniprot/uniprot_sprot_parsed.pkl.gz').substitute()
    RW_FILE = PathTemplate('$rsrc/lite/community/key2cmnt.pkl').substitute()

    def __init__(self, load=True):
        if load:
            data = self.load()
        else:
            data = self.generate()
        super().__init__(data)

    @classmethod
    def load(cls):
        with open(cls.RW_FILE, 'rb') as file:
            return pickle.load(file)

    @classmethod
    def generate(cls):
        Community.import_cache_if_empty(verbose=True)
        Community.load_entries()
        for cmnt in Community.values():
            for key in cmnt.get_keywords():
                yield key, cmnt

    def dump(self):
        with open(self.RW_FILE, 'wb') as file:
            pickle.dump(dict(self), file)
