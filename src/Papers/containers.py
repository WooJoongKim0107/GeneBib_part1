import re
import tarfile, gzip, pickle
from textwrap import dedent
from functools import cache
import webbrowser
from myclass import MetaCacheExt
from mypathlib import PathTemplate


class ISSN(str):
    _isvalid = re.compile('^[0-9]{4}-[0-9]{3}[0-9xX]$').match

    def __new__(cls, issn: str):
        assert cls._isvalid(issn), f'Given ISSN, "{issn}", is not valid'
        return super().__new__(cls, issn)

    def __eq__(self, other):
        return super().__eq__(other)

    def __repr__(self):
        return f'{type(self).__name__}({super().__repr__()})'

    def __hash__(self):
        return super().__hash__()


class Journal(metaclass=MetaCacheExt):
    """
    .medline_ta:
        States the title abbreviation for the journal in which the article appeared.
        These title abbreviations are designated by NLM.
        Location: /PubmedArticleSet/PubmedArticle/MedlineCitation/MedlineJournalInfo/MedlineTA

    .full_titles: set of all occurrences of 'full_title' for each JournalTitle object
        Provides the full journal title, as recorded in the NLM cataloging data
         following the NLM serial title standardization.
        Also provdes the title for referencce lists.
        Location: /PubmedArticleSet/PubmedArticle/MedlineCitation/Article/Journal/Title

    .iso_abbreviations: set of all occurrences of 'iso_abbreviation' for each JournalTitle object
        Contains the NLM version of the journal title ISO Abbreviation.
        Location: /PubmedArticleSet/PubmedArticle/MedlineCitation/Article/Journal/ISOAbbreviation

    issn_p/e/l: ISSN of type 'Print'/'Electronic'/'Linking' of the journal
        Indicates the ISSN of the journal in which the article was published.
        The @IssnType attribute indicates whether the ISSN is the print ISSN or the electronic ISSN.
        A separate element, ISSNLinking indicates the linking ISSN.

    nlm_unique_id:
        Specifies the accession number, a unique identifier for the journal,
        as assigned to the journal's catalog record by NLM.
    """
    _CACHE_PATH = PathTemplate('$rsrc/pdata/paper/journal_cache.pkl.gz').substitute()
    _ARTICLE_PATH = PathTemplate('$rsrc/pdata/paper/sorted/$journal.pkl.gz')
    _TAR_PATH = PathTemplate('$rsrc/pdata/paper/papers.tar').substitute()
    _PATTERN = re.compile(r'[^a-zA-Z0-9_]+')

    def __new__(cls, medline_ta: str, caching=True):
        """__new__ method must not be skipped - Assertion of MetaCacheExt"""
        return super().__new__(cls)

    def __init__(self, medline_ta: str, caching=True):
        """
        # 7?????? issn_p??? 15?????? Journal instance??? ?????? ?????? -> ???????????? ????????? ?????????
        # 2?????? issn_e??? 4?????? Journal instance??? ?????? ?????? -> ???????????? ????????? ?????????
        # 49?????? issn_l??? 102?????? Journal instance??? ?????? ?????? -> 10% ???????????? ????????? ?????????
        """
        self.medline_ta: str = medline_ta
        self.full_titles: set[str] = set()  # len==0: Null, len==1: ?????????, len>1: 141
        self.iso_abbreviations: set[str] = set()  # len==0: 6???, len==1: ?????????

        self.issn_ps: set[ISSN] = set()  # len==0: 7475, len>1: 83
        self.issn_es: set[ISSN] = set()  # len==0: 25683, len>1: 1
        self.issn_ls: set[ISSN] = set()  # len==0: 5200, len>1: 85

        self.nlm_unique_id: set[str] = set()  # len==0: Null, len>1: 157

        self.extra_attrs = {}
        self.counts = 0

    @classmethod
    def from_parse(cls,
                   medline_ta: str,
                   nlm_unique_id: str | None,
                   issn: tuple[ISSN | None, str],
                   issn_l: tuple[ISSN | None, str],
                   title: str | None,
                   iso_abbreviation: str | None):
        """Alternate method to create Journal instance from parsed xml data"""

        new = cls(choose_title(medline_ta, iso_abbreviation, title))
        attach(new, nlm_unique_id, issn, issn_l, title, iso_abbreviation)
        return new

    def merge(self, j):
        """Method to merge another Journal instance to itself
        This method must not be deleted or renamed - Assertion of MetaCacheExt"""
        self.full_titles.update(j.full_titles)
        self.iso_abbreviations.update(j.iso_abbreviations)
        self.issn_ps.update(j.issn_ps)
        self.issn_es.update(j.issn_es)
        self.issn_ls.update(j.issn_ls)
        self.nlm_unique_id.update(j.nlm_unique_id)
        self.counts += j.counts

    @classmethod
    def unique_keys(cls):
        return [k for k, v in cls.unique_items()]

    @classmethod
    def unique_values(cls):
        return [v for k, v in cls.unique_items()]

    @classmethod
    @cache
    def unique_items(cls):
        return [(k, v) for k, v in cls.items() if k == v.medline_ta]

    @property
    def issns(self):
        return set.union(self.issn_ps, self.issn_es, self.issn_ls)

    @property
    def titles(self):
        return {self.medline_ta}.union(self.full_titles, self.iso_abbreviations)

    @property
    def url(self):
        url = '    '.join(f'https://ncbi.nlm.nih.gov/nlmcatalog/{nlm_id}/' for nlm_id in self.nlm_unique_id)
        return url

    @property
    def _simple_title(self):
        return self._PATTERN.sub('_', self.medline_ta)

    @property
    def path(self):
        return self._ARTICLE_PATH.substitute(journal=self._simple_title)

    @property
    def articles(self):
        with gzip.open(self.path) as f:
            counts = pickle.load(f) - 1
            key = pickle.load(f)
            assert counts == self.counts
            assert key == self.medline_ta
            for _ in range(counts):
                yield pickle.load(f)

    @property
    def info(self):
        return dedent(f"""\
        {self}
            Title:
                       MedlineTA: {self.medline_ta}
                           Title: {_print_set(self.full_titles)}
                ISOAbbreviations: {_print_set(self.iso_abbreviations)}

            ISSN:
                           Print: {_print_set(self.issn_ps)}
                      Electronic: {_print_set(self.issn_es)}
                         Linking: {_print_set(self.issn_ls)}

                     NlmUniqueID: {_print_set(self.nlm_unique_id)}
                            URLs: {self.url}
                          Counts: {self.counts}
        """)

    def __repr__(self):
        return f"{type(self).__name__}({self.medline_ta})"

    def __getnewargs__(self):
        """__getnewargs__ must not be deleted - Assertion of MetaCacheExt"""
        return self.medline_ta, False

    def __bool__(self):
        return self.medline_ta

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.medline_ta)

    @classmethod
    def findall(cls, val):
        target = str(val).lower()
        for v in cls.values():
            if target in v.info.lower():
                yield v

    @classmethod
    def find_title(cls, val):
        target = str(val).lower()
        for v in cls.values():
            if target in v.titles.lower():
                yield v

    @classmethod
    def find_issn(cls, val):
        val = ISSN(val)
        for v in cls.values():
            if val in v.issns:
                yield v

    @classmethod
    def find_nlm_id(cls, val):
        for v in cls._CACHE.values():
            if val in v.nlm_unique_id:
                yield v


if Journal._CACHE_PATH.is_file():
    Journal.import_cache(verbose=True)


class Article:
    def __init__(self, pmid: int):
        self.pmid: int = pmid
        self._journal_title: str = ''
        self.pub_date: dict[str: str|int] = {}

        self.title: str = ''
        self.abstract: str = ''
        self.location: int = -1

    @classmethod
    def from_parse(cls,
                   pmid: int,
                   pub_date: dict[str: str|int],
                   title: str,
                   abstract: str):

        new = cls(pmid)
        new.pub_date = pub_date
        new.title = title
        new.abstract = abstract
        return new

    @property
    def journal(self):
        return Journal[self._journal_title]

    @property
    def info(self):
        return dedent(f"""\
        {self}
            Journal: {self._journal_title}
            PubDate: {self.pub_date}
           Location: {self.location}
               PMID: {self.pmid}
                URL: {self.url}
              Title: {self.title}
           Abstract: {self.abstract}
        """)

    @property
    def url(self):
        return f'https://pubmed.ncbi.nlm.nih.gov/{self.pmid}/'

    def browse(self):
        webbrowser.open(self.url)

    def __repr__(self):
        return f"Article({self.pmid})"

    def __getnewargs__(self):
        return self.pmid,

    def __bool__(self):
        return True


def choose_title(medline_ta: str, iso_abbreviation: str|None, title: str|None):
    if medline_ta:
        return medline_ta
    elif iso_abbreviation:
        # Total 4 cases found that <MedlineTA>.text is empty
        # And no cases found that .medline_ta != .iso_abbreviations except those 4
        return iso_abbreviation
    elif title:
        return title
    else:
        print("No proper title has been found. Initiated with Journal('_Anonymous_')")
        return '_Anonymous_'


def attach(self,
           nlm_unique_id: str|None = None,
           issn: tuple[ISSN|None, str] = (None, ''),
           issn_l: tuple[ISSN|None, str] = (None, ''),
           title: str|None = None,
           iso_abbreviation: str|None = None):

    if nlm_unique_id:
        self.nlm_unique_id.add(nlm_unique_id)
    if issn[0]:
        attach_issn(self, issn)
    if issn_l[0]:
        attach_issn(self, issn_l)
    if title:
        self.full_titles.add(title)
    if iso_abbreviation:
        self.iso_abbreviations.add(iso_abbreviation)


def attach_issn(self, issn: tuple[ISSN, str]):
    match issn:
        case (ISSN(x), 'Print'):
            self.issn_ps.add(x)
        case (ISSN(x), 'Electronic'):
            self.issn_es.add(x)
        case (ISSN(x), 'Linking'):
            self.issn_ls.add(x)
        case _:
            raise ValueError(f'Failed to attach ISSN: {issn}')


def _print_set(s: set):
    if len(s) == 1:
        return str(next(iter(s)))
    elif s:
        return s
    else:
        return ''
