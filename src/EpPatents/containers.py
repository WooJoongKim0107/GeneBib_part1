import gzip
import pickle
from textwrap import dedent
from mypathlib import PathTemplate
from myfunc.modtxt import capture


class EpPatent:
    PARSED_PATH = PathTemplate('$pdata/ep_patent/patent_$index.pkl.gz')
    _CPC_PATH = PathTemplate('$lite/us_patent/cpc_tree.pkl').substitute()
    _CPC_TREE_PATH = PathTemplate('$lite/us_patent/cpc_selected.pkl').substitute()
    _IPC_TREE_PATH = PathTemplate('$lite/us_patent/ipc_selected.pkl').substitute()
    _TREE_ = None

    def __init__(self, pub_number: str):
        self.pub_number: str = pub_number
        self.app_number: str = ''
        self.title: str = ''
        self.abstract: str = ''

        self.filing_date: dict[str, int] = {'Year': 0, 'Month': 0, 'Date': 0}
        self.pub_date: dict[str, int] = {'Year': 0, 'Month': 0, 'Date': 0}
        self.grant_date: dict[str, int] = {'Year': 0, 'Month': 0, 'Date': 0}

        self.cpcs: set[str] = set()
        self.citations: set[str] = set()
        self.location: int = -1

    @classmethod
    def load(cls, index):
        """0 <= index < 112"""
        with gzip.open(cls.PARSED_PATH.substitute(index=index), 'rb') as file:
            return pickle.load(file)

    @classmethod
    def _load_cpc(cls):
        if not cls._TREE_:
            from UsPatents.cpc import CPCTree
            cls._TREE_ = CPCTree(load=True)

    @classmethod
    def _load_ipc(cls):
        if not cls._TREE_:
            from UsPatents.cpc import IPCTree
            cls._TREE_ = IPCTree(load=True)

    @classmethod
    def load_selected(cls, index, use_cpc: bool, load=True):
        cls._load_cpc() if use_cpc else cls._load_ipc()
        chain = cls.load(index)
        res = {pub_number for pub_number, ep_pat in chain.items() if cls._TREE_.any_selected(ep_pat.cpcs)}
        if not load:
            del cls._TREE_
        return res

    @property
    def is_granted(self):
        return self.grant_date != {'Year': 0, 'Month': 0, 'Date': 0}

    @classmethod
    def from_parse(cls,
                   pub_number: str,
                   app_number: str,
                   title: str,
                   abstract: str,

                   filing_date: dict[str, int],
                   pub_date: dict[str, int],
                   grant_date: dict[str, int],

                   cpcs: set[str],
                   citations: set[str]):
        if pub_number:
            new = cls(pub_number)
        elif app_number:
            new = cls(app_number)
        else:
            print("Neither publication nor application number provided. Initiated with EpPatent('_Anonymous_')'")
            new = cls('_Anonymous_')

        new.app_number = app_number
        new.title = title
        new.abstract = abstract

        if filing_date:
            new.filing_date = filing_date
        if pub_date:
            new.pub_date = pub_date
        if grant_date:
            new.grant_date = grant_date

        if cpcs:
            new.cpcs = cpcs
        if citations:
            new.citations = citations
        return new

    @property
    def info(self):
        return dedent(f"""\
        {self}
          application: {self.app_number}  {self.filing_date}
          publication: {self.pub_number}  {self.pub_date}
                grant: {self.grant_date if self.is_granted else 'Not granted'}

             Citation: total {len(self.citations)}; {_print_set(self.citations)}
            CPC codes: {_print_set(self.cpcs)}

             Location: {self.location}
               Number: publication={self.pub_number}, application={self.app_number}
                Title: {self.title}
             Abstract: {self.abstract}
        """)

    def info_with(self, *txts):
        return dedent(f"""\
        {self}
          application: {self.app_number}  {self.filing_date}
          publication: {self.pub_number}  {self.pub_date}
                grant: {self.grant_date if self.is_granted else 'Not granted'}

             Citation: total {len(self.citations)}; {_print_set(self.citations)}
            CPC codes: {_print_set(self.cpcs)}

             Location: {self.location}
               Number: publication={self.pub_number}, application={self.app_number}
                Title: {capture(self.title, *txts)}
             Abstract: {capture(self.abstract, *txts)}
        """)

    def __repr__(self):
        return f"EpPatent({self.pub_number})"


def _print_set(s: set):
    if len(s) == 1:
        return str(next(iter(s)))
    elif s:
        return s
    else:
        return ''
