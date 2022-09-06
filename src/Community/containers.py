from UniProt.containers import UniProtEntry
from myclass.cached_class import MetaCacheExt
from mypathlib import PathTemplate


class Community(metaclass=MetaCacheExt):
    _CACHE_PATH = PathTemplate('$rsrc/pdata/community/community_cache.pkl.gz').substitute()

    def __new__(cls, cmnt_idx: int, caching=True):
        """__new__ method must not be skipped - Assertion of MetaCacheExt"""
        return super().__new__(cls)

    def __init__(self, cmnt_idx: int, caching=True):
        self.cmnt_idx: int = cmnt_idx
        self.entries: list[UniProtEntry] = []
        self.added: list[str] = []
        self.removed: list[str] = []
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

    @property
    def key_accs(self):
        return [ent.key_acc for ent in self.entries]

    @property
    def accessions(self):
        return [acc for ent in self.entries for acc in ent.accessions]

    @property
    def keywords(self):
        return [key for ent in self.entries for key in ent.keywords]

    @property
    def references(self):
        return [ref for ent in self.entries for ref in ent.references]
