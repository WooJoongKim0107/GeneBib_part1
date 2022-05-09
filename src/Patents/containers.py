from textwrap import dedent
from Papers.containers import _print_set


class Patent:
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

    @property
    def is_granted(self):
        return self.grant_date == {'Year': 0, 'Month': 0, 'Date': 0}

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
            print("Neither publication nor application number provided. Initiated with Patent('_Anonymous_')'")
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

    def info(self):
        return dedent(f"""\
        {self}
            Number
                publication: {self.pub_number}
                application: {self.app_number}
                    granted: {'Yes' if self.is_granted else 'No'}

            Date
                application: {self.filing_date}
                publication: {self.pub_date}
                      grant: {self.grant_date}
                      
            Citation
                      count: {len(self.citations)}
                      lists: {_print_set(self.citations)}
                      
            CPC codes: {_print_set(self.cpcs)}
                
            Title: {self.title}
            
            Abstract: {self.abstract}
        """)

    def __repr__(self):
        return f"Patent({self.pub_number})"
