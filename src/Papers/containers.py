import re
import pickle
import gzip
from textwrap import dedent
from mypathlib import PathTemplate


class _BaseISSN(str):
    _isvalid = re.compile('^[0-9]{4}-[0-9]{3}[0-9xX]$').match

    def __new__(cls, issn: str):
        assert cls._isvalid(issn), f'Given ISSN, "{issn}", is not valid'
        return super().__new__(cls, issn)

    def __eq__(self, other):
        return (type(self) is type(other)) and super().__eq__(other)

    def __repr__(self):
        return f'{type(self).__name__}({super().__repr__()})'

    def __hash__(self):
        return super().__hash__()


class ISSNp(_BaseISSN):
    pass


class ISSNe(_BaseISSN):
    pass


class ISSNl(_BaseISSN):
    pass


ISSN = ISSNp|ISSNe|ISSNl
_ISSN_FACTORY = {'Print': ISSNp, 'Electronic': ISSNe, 'Linking': ISSNl}
_ISSN_INVERSE = {ISSNp: 'Print', ISSNe: 'Electronic', ISSNl: 'Linking'}


def issn_factory(issn_val: str, issn_type: str):
    cls = _ISSN_FACTORY[issn_type]
    return cls(issn_val)


class Journal:
    """
    .medline_ta:
        States the title abbreviation for the journal in which the article appeared.
        These title abbreviations are designated by NLM.
        Location: /PubmedArticleSet/PubmedArticle/MedlineCitation/MedlineJournalInfo/MedlineTA

    .full_titles: set of all occurrences of 'full_title' for each JournalTitle object.
    full_title:
        Provides the full journal title, as recorded in the NLM cataloging data
         following the NLM serial title standardization.
        Also provdes the title for referencce lists.
        Location: /PubmedArticleSet/PubmedArticle/MedlineCitation/Article/Journal/Title

    .iso_abbreviations: set of all occurrences of 'iso_abbreviation' for each JournalTitle object.
    iso_abbreviation:
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
    _CACHE = {}
    _CACHE_PATH = PathTemplate('$rsrc/pdata/paper/journal_cache.pkl.gz').substitute()

    def __new__(cls, medline_ta: str):
        if medline_ta in cls._CACHE:
            return cls._CACHE[medline_ta]
        return super().__new__(cls)

    def __init__(self, medline_ta: str):
        if medline_ta in self._CACHE:
            return
        self._CACHE[medline_ta] = self

        self.medline_ta: str = medline_ta
        self.full_titles: set[str] = set()  # len==0: Null, len==1: 대부분, len>1: 141
        self.iso_abbreviations: set[str] = set()  # len==0: 6개, len==1: 나머지

        self.issn_ps: set[ISSNp] = set()  # len==0: 7475, len>1: 83
        self.issn_es: set[ISSNe] = set()  # len==0: 25683, len>1: 1
        self.issn_ls: set[ISSNl] = set()  # len==0: 5200, len>1: 85

        self.nlm_unique_id: set[str] = set()  # len==0: Null, len>1: 157

        self.titles = {'MedlineTA': self.medline_ta, 'Title': self.full_titles, 'ISOAbbreviation': self.iso_abbreviations}
        self.issns = {'Print': self.issn_ps, 'Electronic': self.issn_es, 'Linking': self.issn_ls}

    @classmethod
    def from_parse(cls,
                   medline_ta: str,
                   nlm_unique_id: str|None,
                   issn: ISSN|None,
                   issn_l: ISSNl|None,
                   title: str|None,
                   iso_abbreviation: str|None):
        if medline_ta:
            new = cls(medline_ta)
        elif iso_abbreviation:
            # Total 4 cases found that <MedlineTA>.text is empty
            # And no cases found that .medline_ta != .iso_abbreviations except those 4
            new = cls(iso_abbreviation)
        elif title:
            new = cls(title)
        else:
            print("No proper title has been found. Initiated with Journal('_Anonymous_')")
            new = cls('_Anonymous_')

        new.attach(nlm_unique_id, issn, issn_l, title, iso_abbreviation)
        return new

    def attach(self,
               nlm_unique_id: str|None = None,
               issn: ISSN|None = None,
               issn_l: ISSNl|None = None,
               title: str|None = None,
               iso_abbreviation: str|None = None):

        if nlm_unique_id:
            self.nlm_unique_id.add(nlm_unique_id)
        if issn:
            self.attach_issn(issn)
        if issn_l:
            self.attach_issn(issn_l)
        if title:
            self.full_titles.add(title)
        if iso_abbreviation:
            self.iso_abbreviations.add(iso_abbreviation)

    def attach_issn(self, issn: ISSN):
        issn_type = _ISSN_INVERSE[type(issn)]
        self.issns[issn_type].add(issn)

    @classmethod
    def export_cache(cls, path=_CACHE_PATH):
        with gzip.open(path, 'wb') as file:
            pickle.dump(cls._CACHE, file)

    @classmethod
    def import_cache(cls, path=_CACHE_PATH):
        with gzip.open(path, 'rb') as file:
            cls._CACHE.update(pickle.load(file))

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

            NLM ID:
                     NlmUniqueID: {_print_set(self.nlm_unique_id)}
        """)

    @classmethod
    def merge_caches(cls, *caches):
        anchor = caches[0].copy()
        for cache in caches[1:]:
            cls.update_cache(anchor, cache)
        return anchor

    @staticmethod
    def update_cache(c0: dict, c1: dict):
        intersections = c0.keys() & c1.keys()
        for key in intersections:
            c0[key].merge_journals(c1[key])

        for k1, j1 in c1.items():
            if k1 not in intersections:
                c0[k1] = j1


    def merge_journals(self, j):
        self.full_titles.update(j.full_titles)
        self.iso_abbreviations.update(j.iso_abbreviations)
        self.issn_ps.update(j.issn_ps)
        self.issn_es.update(j.issn_es)
        self.issn_ls.update(j.issn_ls)
        self.nlm_unique_id.update(j.nlm_unique_id)

    def __repr__(self):
        return f"{type(self).__name__}({self.medline_ta})"

    def __getnewargs__(self):
        return self.medline_ta,

    def __bool__(self):
        return self.medline_ta

if Journal._CACHE_PATH.is_file():
    Journal.import_cache()


def _print_set(s: set):
    if len(s) == 1:
        return str(next(iter(s)))
    elif s:
        return s
    else:
        return ''


class Article:
    def __init__(self, pmid: int):
        self.pmid: int = pmid
        self.journal: Journal = Journal('')
        self.pub_date: dict[str: str|int] = {}

        self.article_title: str = ''
        self.abstract: str = ''
        self.location: int = -1

    @classmethod
    def from_parse(cls,
                   pmid: int,
                   pub_date: dict[str: str|int],
                   article_title: str,
                   abstract: str):

        new = cls(pmid)
        new.pub_date = pub_date
        new.article_title = article_title
        new.abstract = abstract
        return new

    def info(self):
        return dedent(f"""\
        {self}
            Journal: {self.journal.medline_ta}
            PubDate: {self.pub_date}
            
           Location: {self.location}
               PMID: {self.pmid}
              Title: {self.article_title}
           Abstract: {self.abstract}
        """)

    def __repr__(self):
        return f"Article({self.pmid})"

    def __getnewargs__(self):
        return self.pmid,

    def __bool__(self):
        return True
