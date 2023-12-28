import pickle
from random import choices
from collections.abc import Iterable
from textwrap import dedent, indent
from myfunc import read_commented
from myclass import MetaCacheExt, MetaDisposal, TarRW
from mypathlib import PathTemplate
from UniProt.containers import Match, KeyWord
from StringMatching.base import unify, tokenize
from Papers.containers import Article
from UsPatents.containers import UsPatent
from CnPatents.containers import CnPatent
from EpPatents.containers import EpPatent


class C2K(dict):
    """
    C2K.load() -> dict[int, set[str]]
    , or {cmnt_idx: int -> {unify(keyword): str}}
    """
    PATH = PathTemplate('$data/community/cmnt_to_keyw_matchform_${date}.pkl')
    DATES = ['200500', '220915', '221005', '221018', '221022', '221027', '221123', '231201']

    @classmethod
    def load(cls, date: str | int = -1) -> dict[int, set[str]]:
        """
        :param date: (str), load from $data/community/cmnt_to_keyw_matchform_${date}.pkl
                     (int), load from cls.DATES[date], so must be -len(cls.DATES) <= date < len(cls.DATES)
        """
        if isinstance(date, int):
            date = cls.DATES[date]
        with open(cls.PATH.substitute(date=date), 'rb') as file:
            return pickle.load(file)

    @classmethod
    def load_k2c(cls, date: str | int = -1) -> dict[str, int]:
        return {v: k for k, vs in cls.load(date).items() for v in vs}

    @classmethod
    def dump(cls, obj, date: str):
        """YOU MUST ADD 'date' to C2K.DATES MANUALLY AFTER THIS"""
        for c, ks in obj.items():
            for k in ks:
                if unify(k) != k:
                    raise ValueError(f'{c} has invalid key: {k} != {unify(k) = }')
        size0 = sum(len(es) for es in obj.values())
        size1 = len(set().union(*obj.values()))
        valid_partition = size0 == size1
        if not valid_partition:
            raise ValueError(f"Given obj is not a valid partition.\n{size0-size1} elements exist in more than 1 cmnts.")
        with open(cls.PATH.substitute(date=date), 'wb') as file:
            return pickle.dump(obj, file)


class C2E(dict):
    """
    C2E.load() -> dict[int, tuple[str]]
    , or {cmnt_idx: int -> (key_accession: str)}

    Sorted by its key
    """
    PATH = PathTemplate('$data/community/update_curated_cmnt_map_${date}.pkl')
    DATES = ['220914', '231201']

    @classmethod
    def load(cls, date: str | int = -1) -> dict[int, tuple[str]]:
        """
        :param date: (str), load from $data/community/update_curated_cmnt_map_${date}.pkl
                     (int), load from cls.DATES[date], so must be -len(cls.DATES) <= date < len(cls.DATES)
        """
        if isinstance(date, int):
            date = cls.DATES[date]
        with open(cls.PATH.substitute(date=date), 'rb') as file:
            return pickle.load(file)

    @classmethod
    def load_e2c(cls, date: str | int = -1) -> dict[str, int]:
        return {v: k for k, vs in cls.load(date).items() for v in vs}

    @classmethod
    def dump(cls, obj, date: str):
        """YOU MUST ADD 'date' to C2E.DATES MANUALLY AFTER THIS"""
        size0 = sum(len(es) for es in obj.values())
        size1 = len(set().union(*obj.values()))
        valid_partition = size0 == size1
        if not valid_partition:
            raise ValueError(f"Given obj is not a valid partition.\n{size0-size1} elements exist in more than 1 cmnts.")
        with open(cls.PATH.substitute(date=date), 'wb') as file:
            return pickle.dump(obj, file)


class CmntFinder:
    @classmethod
    def safe_keyword(cls, *keys, strict=True):
        cmnts_of_interest = []
        for key in keys:
            match list(cls.keyword(key, strict=strict)):
                case []:
                    raise ValueError(f'No occurrences found for {key}, try another key')
                case [c]:
                    cmnts_of_interest.append(c)
                case [c, *_]:
                    raise ValueError(f'Multiple occurrences found for {key}, try another key with strict=True')
        return cmnts_of_interest

    @staticmethod
    def entry(*key_accs):
        key_accs = set(key_accs)
        for c in Community.values():
            if not key_accs.isdisjoint(c.entries):
                yield c

    @classmethod
    def keyword(cls, *keys, strict=True):
        return cls._keyword_strict(*keys) if strict else cls._keyword_loose(*keys)

    @staticmethod
    def article(*pmids):
        pmids = set(pmids)
        for c in Community.values():
            if not all(pmids.isdisjoint(v) for v in c.pmids.values()):
                yield c

    @staticmethod
    def us_patent(*pub_numbers):
        pub_numbers = set(pub_numbers)
        for c in Community.values():
            if not all(pub_numbers.isdisjoint(v) for v in c.us_pub_numbers.values()):
                yield c

    @staticmethod
    def cn_patent(*pub_numbers):
        pub_numbers = set(pub_numbers)
        for c in Community.values():
            if not all(pub_numbers.isdisjoint(v) for v in c.cn_pub_numbers.values()):
                yield c

    @staticmethod
    def ep_patent(*pub_numbers):
        pub_numbers = set(pub_numbers)
        for c in Community.values():
            if not all(pub_numbers.isdisjoint(v) for v in c.ep_pub_numbers.values()):
                yield c

    @staticmethod
    def _keyword_strict(*keys):
        keys = set(keys)
        for c in Community.values():
            if not keys.isdisjoint(c.keywords):
                yield c

    @staticmethod
    def _keyword_loose(*keys):
        keys = {key.lower() for key in keys}
        for c in Community.values():
            c: Community
            if any(key.lower() in keys for key in c.keywords):
                yield c


class Community(metaclass=MetaCacheExt):
    CACHE_PATH = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()
    ARTICLE_PATH = PathTemplate('$pdata/paper/sorted/community.tar').substitute()
    US_PATENT_PATH = PathTemplate('$pdata/us_patent/sorted/community.tar').substitute()
    CN_PATENT_PATH = PathTemplate('$pdata/cn_patent/sorted/community.tar').substitute()
    EP_PATENT_PATH = PathTemplate('$pdata/ep_patent/sorted/community.tar').substitute()
    PUB_YEAR_PATHS = {'paper': PathTemplate('$lite/paper/pmid2year.pkl').substitute(),
                      'us_patent': PathTemplate('$lite/us_patent/pubnum2year.pkl').substitute(),
                      'cn_patent': PathTemplate('$lite/cn_patent/pubnum2year.pkl').substitute(),
                      'ep_patent': PathTemplate('$lite/ep_patent/pubnum2year.pkl').substitute(),}
    _PUB_YEARS_ = {}
    finder = CmntFinder  # Community.finder.entry('P0C9F0')

    def __init__(self, cmnt_idx: int, caching=True):
        self.cmnt_idx: int = cmnt_idx
        self.entries: list[str] = []
        self.keywords: set[str] = set()
        self.pmids: dict[str, set[int]] = {}
        self.us_pub_numbers: dict[str, set[str]] = {}
        self.cn_pub_numbers: dict[str, set[str]] = {}  # pub_numbers for Chinese patents from EPO PATSTAT
        self.ep_pub_numbers: dict[str, set[str]] = {}  # pub_numbers for European patents from EPO PATSTAT

    @classmethod
    def random_infos_of_interest(cls, k, *keys, strict=True):
        """See random_infos_of_interest_easy  if you want to print it for curation"""
        for c in cls.finder.safe_keyword(*keys, strict=strict):
            ps, us_ts, cn_ts, ep_ts = c.random_materials(k)
            ps = [p.info_with(*c.pmids) for p in ps]
            us_ts = [ut.info_with(*c.us_pub_numbers) for ut in us_ts]
            cn_ts = [ct.info_with(*c.cn_pub_numbers) for ct in cn_ts]
            ep_ts = [et.info_with(*c.ep_pub_numbers) for et in ep_ts]
            yield c, ps, us_ts, cn_ts, ep_ts

    def get_first(self, load=True):
        """set load=True if you want to call Community.get_first_pubs() repeatedly"""
        self._load_pub_years()
        pmid2year = self._PUB_YEARS_['paper']
        us_pubnum2year = self._PUB_YEARS_['us_patent']
        cn_pubnum2year = self._PUB_YEARS_['cn_patent']
        ep_pubnum2year = self._PUB_YEARS_['ep_patent']

        pmids = ((key, pmid) for key, pmids in self.pmids.items() for pmid in pmids)
        us_pubnums = ((key, pub) for key, pubs in self.us_pub_numbers.items() for pub in pubs)
        cn_pubnums = ((key, pub) for key, pubs in self.cn_pub_numbers.items() for pub in pubs)
        ep_pubnums = ((key, pub) for key, pubs in self.ep_pub_numbers.items() for pub in pubs)
        pmids = ((key, pmid, pmid2year[pmid]) for key, pmid in pmids if pmid in pmid2year)
        us_pubnums = ((key, pub, us_pubnum2year[pub]) for key, pub in us_pubnums if pub in us_pubnum2year)
        cn_pubnums = ((key, pub, cn_pubnum2year[pub]) for key, pub in cn_pubnums if pub in cn_pubnum2year)
        ep_pubnums = ((key, pub, ep_pubnum2year[pub]) for key, pub in ep_pubnums if pub in ep_pubnum2year)

        p_key, pmid, p_year = min(pmids, key=lambda x: x[-1], default=('', -1, 9999))
        us_key, us_pubnum, us_year = min(us_pubnums, key=lambda x: x[-1], default=('', '', 9999))
        cn_key, cn_pubnum, cn_year = min(cn_pubnums, key=lambda x: x[-1], default=('', '', 9999))
        ep_key, ep_pubnum, ep_year = min(ep_pubnums, key=lambda x: x[-1], default=('', '', 9999))

        if not load:
            self._PUB_YEARS_.clear()
        return (p_key, pmid, p_year), (us_key, us_pubnum, us_year), \
               (cn_key, cn_pubnum, cn_year), (ep_key, ep_pubnum, ep_year)

    def get_first_materials(self, load=True):
        """set load=True if you want to call Community.get_first_materials() repeatedly"""
        (p_key, pmid, _), (us_key, us_pubnum, _), (cn_key, cn_pubnum, _), (ep_key, ep_pubnum, _) = self.get_first(load=load)
        return self.get_articles()[pmid], self.get_us_patents()[us_pubnum],\
               self.get_cn_patents()[cn_pubnum], self.get_ep_patents()[ep_pubnum]

    def get_first_infos(self, load=True):
        """set load=True if you want to call Community.get_first_infos() repeatedly"""
        (p_key, pmid, _), (us_key, us_pubnum, _), (cn_key, cn_pubnum, _), (ep_key, ep_pubnum, _) = self.get_first(load=load)
        art, us_pat, cn_pat, ep_pat = self.get_articles()[pmid], self.get_us_patents()[us_pubnum],\
                                      self.get_cn_patents()[cn_pubnum], self.get_ep_patents()[ep_pubnum]
        return art.info_with(p_key), us_pat.info_with(us_key), cn_pat.info_with(cn_key), ep_pat.info_with(ep_key)

    def random_materials(self, k, *texts):
        pmids, us_pubs, cn_pubs, ep_pubs = map(set, self.choices(k, *texts))
        x = self.get_articles()
        arts = [x[pmid] for pmid in pmids]
        x = self.get_us_patents()
        us_pats = [x[pub] for pub in us_pubs]
        x = self.get_cn_patents()
        cn_pats = [x[pub] for pub in cn_pubs]
        x = self.get_ep_patents()
        ep_pats = [x[pub] for pub in ep_pubs]
        return arts, us_pats, cn_pats, ep_pats

    def random_infos(self, k, *texts):
        """
        Capturing text here is simple str.replace(), none of our StringMatching algorithms.
        So, some captures might not be matched.
        """
        arts, us_pats, cn_pats, ep_pats = self.random_materials(k, *texts)
        arts_info = [art.info_with(*texts) for art in arts]
        us_pats_info = [us_pat.info_with(*texts) for us_pat in us_pats]
        cn_pats_info = [cn_pat.info_with(*texts) for cn_pat in cn_pats]
        ep_pats_info = [ep_pat.info_with(*texts) for ep_pat in ep_pats]
        return arts_info, us_pats_info, cn_pats_info, ep_pats_info

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
                        {self.total_us_patent_hits:>7,} hits (US patent)
                        {self.total_cn_patent_hits:>7,} hits (CN patent)
                        {self.total_ep_patent_hits:>7,} hits (EP patent)
        """)

    @property
    def more_info(self):
        return dedent(f"""\
        {self}
               Entries: {_print_set(self.entries)}
              Keywords: {_print_set(self.keywords)}
            Total hits: 
                        {self.total_paper_hits:>7,} hits (paper)
                        {self.total_us_patent_hits:>7,} hits (US patent)
                        {self.total_cn_patent_hits:>7,} hits (CN patent)
                        {self.total_ep_patent_hits:>7,} hits (EP patent)
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

    def hits(self, text: str) -> tuple[int, int, int, int]:
        """return (total_paper_hits, total_patent_hits)"""
        return self.paper_hits.get(text, 0), self.us_patent_hits.get(text, 0),\
               self.cn_patent_hits.get(text, 0), self.ep_patent_hits.get(text, 0)

    @property
    def total_hits(self) -> int:
        """return total number of hits (regardless of material type)"""
        return self.total_paper_hits + self.total_us_patent_hits + self.total_cn_patent_hits + self.total_ep_patent_hits

    def hit_summary(self) -> dict[str, tuple[int, int, int]]:
        """{match.text -> (total_paper_hits, total_patent_hits)} sorted by paper_hits and patent_hits"""
        keys = sorted(self.paper_hits | self.us_patent_hits | self.cn_patent_hits | self.ep_patent_hits,
                      key=self.hits, reverse=True)
        return {k: self.hits(k) for k in keys}

    def get_articles(self) -> dict[int, Article]:
        if not self.total_paper_hits:
            return {}
        with TarRW(self.ARTICLE_PATH, 'r') as tf:
            return tf.gzip_load(f'{self.cmnt_idx}.pkl.gz')  # dict[pmid, Article]

    @property
    def us_patent_hits(self) -> dict[str, int]:
        """dict[match.text, num_hits]"""
        return {k: len(v) for k, v in self.us_pub_numbers.items()}

    @property
    def total_us_patent_hits(self) -> int:
        return sum(self.us_patent_hits.values())

    def get_us_patents(self) -> dict[str, UsPatent]:
        if not self.total_us_patent_hits:
            return {}
        with TarRW(self.US_PATENT_PATH, 'r') as tf:
            return tf.gzip_load(f'{self.cmnt_idx}.pkl.gz')  # dict[pub_number, UsPatent]

    @property
    def cn_patent_hits(self) -> dict[str, int]:
        """dict[match.text, num_hits]"""
        return {k: len(v) for k, v in self.cn_pub_numbers.items()}

    @property
    def total_cn_patent_hits(self) -> int:
        return sum(self.cn_patent_hits.values())

    def get_cn_patents(self) -> dict[str, CnPatent]:
        if not self.total_cn_patent_hits:
            return {}
        with TarRW(self.CN_PATENT_PATH, 'r') as tf:
            return tf.gzip_load(f'{self.cmnt_idx}.pkl.gz')  # dict[pub_number, CnPatent]
        
    @property
    def ep_patent_hits(self) -> dict[str, int]:
        """dict[match.text, num_hits]"""
        return {k: len(v) for k, v in self.ep_pub_numbers.items()}

    @property
    def total_ep_patent_hits(self) -> int:
        return sum(self.ep_patent_hits.values())

    def get_ep_patents(self) -> dict[str, EpPatent]:
        if not self.total_ep_patent_hits:
            return {}
        with TarRW(self.EP_PATENT_PATH, 'r') as tf:
            return tf.gzip_load(f'{self.cmnt_idx}.pkl.gz')  # dict[pub_number, UsPatent]

    # Fine-details
    def __repr__(self):
        return f"Community({self.cmnt_idx})"

    def __bool__(self):
        return bool(self.total_hits)

    def _more_info(self):
        summary = self.hit_summary()
        form = '{:,}'.format
        ps, ts, es = zip(*(map(form, x) for x in summary.values()))
        return indent(col_prints(summary, ps, ts, es, sep=' '), ' '*16)

    def choices(self, k, *texts):
        if texts:
            pmids = [pmid for text in texts for pmid in self.pmids[text]]
            us_pubs = [pub for text in texts for pub in self.us_pub_numbers[text]]
            cn_pubs = [pub for text in texts for pub in self.cn_pub_numbers[text]]
            ep_pubs = [pub for text in texts for pub in self.ep_pub_numbers[text]]
        else:
            pmids = [x for v in self.pmids.values() for x in v]
            us_pubs = [x for v in self.us_pub_numbers.values() for x in v]
            cn_pubs = [x for v in self.cn_pub_numbers.values() for x in v]
            ep_pubs = [x for v in self.ep_pub_numbers.values() for x in v]
        return choices(pmids, k=k), choices(us_pubs, k=k), choices(cn_pubs, k=k), choices(ep_pubs, k=k)

    @classmethod
    def _load_pub_years(cls):
        if not cls._PUB_YEARS_:
            for mode, path in cls.PUB_YEAR_PATHS.items():
                with open(path, 'rb') as file:
                    cls._PUB_YEARS_[mode] = pickle.load(file)

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
        for k, v in cmnt.us_pub_numbers.items():
            self.us_pub_numbers.setdefault(k, set()).update(v)
        for k, v in cmnt.cn_pub_numbers.items():
            self.cn_pub_numbers.setdefault(k, set()).update(v)
        for k, v in cmnt.ep_pub_numbers.items():
            self.ep_pub_numbers.setdefault(k, set()).update(v)

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
    def all_equivalents(cls, keywords: Iterable[str]):
        """.load() or .safe_load() must be called before .all_equivalents()"""
        it = (cls.DATA[k] for k in keywords if k in cls.DATA)  # Remove keywords not in parsed UniProtDB
        it = (x for x in it if all(KeyFilter.isvalid(v) for v in x))  # Remove filtered
        return set().union(*it)


class KeyFilter(metaclass=MetaDisposal):
    R_FILE = PathTemplate('$data/curations/filtered/filtered_key.txt').substitute()
    DATA = set()

    @classmethod
    def load(cls):
        with open(cls.R_FILE, 'r') as file:  # Read
            for line in read_commented(file):
                cls.DATA.add(line)

    @classmethod
    def isvalid(cls, k: str):
        return k not in cls.DATA


class TextFilter(metaclass=MetaDisposal):
    R_FILE = PathTemplate('$data/curations/filtered/filtered.txt').substitute()
    DATA = set()

    @classmethod
    def load(cls):
        with open(cls.R_FILE, 'r') as file:  # Read
            for line in read_commented(file):
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


class CmntFilter(metaclass=MetaDisposal):
    R_FILE = PathTemplate('$data/curations/filtered/filtered_cmnt.txt').substitute()
    DATA = set()

    @classmethod
    def load(cls):
        with open(cls.R_FILE, 'r') as file:  # Read
            for line in read_commented(file):
                cls.DATA.add(int(line))

    @classmethod
    def isvalid(cls, x: str):
        return x not in cls.DATA


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
    _R_FILE1 = PathTemplate('$data/curations/filtered/filtered.txt').substitute()
    _R_FILE2 = PathTemplate('$data/curations/filtered/filtered_cmnt.txt').substitute()
    _R_FILE3 = PathTemplate('$lite/community/key2cmnt.pkl').substitute()
    _R_FILE4 = PathTemplate('$pdata/uniprot/uniprot_keywords.pkl').substitute()

    def __init__(self):
        Community.import_cache_if_empty(verbose=True)  # Read0
        TextFilter.load()  # Read1
        CmntFilter.load()  # Read2
        self.uk2c = UniKey2Cmnt.load()  # Read3
        self.keywords = {k: k for k in KeyWord.load()}  # Read4

    def assign(self, key, attr, *matches):
        already = set()
        for match in matches:
            for cmnt_idx in self.which(match):
                if cmnt_idx not in already and CmntFilter.isvalid(cmnt_idx):
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


def random_infos_of_interest_easy(genes, fname_paper, fname_us_patent, fname_cn_patent, fname_ep_patent, k=30):
    Community.import_cache_if_empty()
    with open(fname_paper, 'w') as pfile:
        with open(fname_us_patent, 'w') as usfile:
            with open(fname_cn_patent, 'w') as cnfile:
                with open(fname_ep_patent, 'w') as epfile:
                    for c, ps, us_ts, cn_ts, ep_ts in Community.random_infos_of_interest(k, *genes, strict=True):
                        pfile.write(c.more_info)
                        usfile.write(c.more_info)
                        cnfile.write(c.more_info)
                        epfile.write(c.more_info)
                        pfile.write('\n')
                        usfile.write('\n')
                        cnfile.write('\n')
                        epfile.write('\n')
                        for p in ps:
                            pfile.write(indent(p, '    '))
                            pfile.write('\n')
                        pfile.write('================================\n')
                        for ut in us_ts:
                            usfile.write(indent(ut, '    '))
                            usfile.write('\n')
                        usfile.write('================================\n')
                        for ct in cn_ts:
                            cnfile.write(indent(ct, '    '))
                            cnfile.write('\n')
                        cnfile.write('================================\n')
                        for et in ep_ts:
                            epfile.write(indent(et, '    '))
                            epfile.write('\n')
                        epfile.write('================================\n')
