import re
import pickle
from textwrap import dedent
from itertools import chain
from collections import namedtuple
from dataclasses import dataclass, field
from myclass import MetaLoad, MetaCacheExt
from mypathlib import PathTemplate
from StringMatching.base import pluralize, tokenize, unify


# NAME_TAGS provides maps to old-style kw_type
NAME_TAGS = {
    ('protein', 'recommendedName', 'fullName'): 0,
    ('protein', 'recommendedName', 'shortName'): 1,
    ('protein', 'recommendedName', 'ecNumber'): 2,  # Not used
    ('protein', 'alternativeName', 'fullName'): 3,
    ('protein', 'alternativeName', 'shortName'): 4,
    ('protein', 'alternativeName', 'ecNumber'): 5,  # Not used
    ('protein', 'allergenName'): 6,
    ('protein', 'cdAntigenName'): 7,
    ('protein', 'innName'): 8,
    ('protein', 'component', 'recommendedName', 'fullName'): 9,  # Not used
    ('protein', 'component', 'recommendedName', 'shortName'): 10,  # Not used
    ('protein', 'component', 'recommendedName', 'ecNumber'): 11,  # Not used
    ('protein', 'component', 'alternativeName', 'fullName'): 12,  # Not used
    ('protein', 'component', 'alternativeName', 'shortName'): 13,  # Not used
    ('protein', 'component', 'alternativeName', 'ecNumber'): 14,  # Not used
    ('protein', 'component', 'allergenName'): 15,  # Not used
    ('protein', 'domain', 'recommendedName', 'fullName'): 16,  # Not used
    ('protein', 'domain', 'recommendedName', 'shortName'): 17,  # Not used
    ('protein', 'domain', 'recommendedName', 'ecNumber'): 18,  # Not used
    ('protein', 'domain', 'alternativeName', 'fullName'): 19,  # Not used
    ('protein', 'domain', 'alternativeName', 'shortName'): 20,  # Not used
    ('protein', 'domain', 'alternativeName', 'ecNumber'): 21,  # Not used
    ('gene', 'name', 'primary'): 22,
    ('gene', 'name', 'synonym'): 23,
    ('gene', 'name', 'ordered locus'): 24,  # Not used
    ('gene', 'name', 'ORF'): 25
}

INV_TAGS = {v: k for k, v in NAME_TAGS.items()}

BLACK_LIST = {2, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 24}

ALPHA2GREEK = {
    "alpha": '\u03b1',      # α, Α
    "beta": '\u03b2',       # β, Β
    "gamma": '\u03b3',      # γ, Γ
    "delta": '\u03b4',      # δ, Δ
    "epsilon": '\u03b5',    # ε, Ε
    "zeta": '\u03b6',       # ζ, Ζ
    "eta": '\u03b7',        # η, Η
    "iota": '\u03b9',       # ι, Ι
    "kappa": '\u03ba',      # κ, Κ
    "lambda": '\u03bb',     # λ, Λ
    "mu": '\u03bc',         # μ, Μ
    "nu": '\u03bd',         # ν, Ν
    "xi": '\u03be',         # ξ, Ξ
    "omicron": '\u03bf',    # ο, Ο
    "pi": '\u03c0',         # π, Π
    "rho": '\u03c1',        # ρ, Ρ
    "tau": '\u03c4',        # τ, Τ
    "upsilon": '\u03c5',    # υ, Υ
    "phi": '\u03c6',        # φ, Φ
    "chi": '\u03c7',        # χ, Χ
    "psi": '\u03c8',        # ψ, Ψ
    "omega": '\u03c9',      # ω, Ω
    "theta": '\u03b8',      # θ, Θ, symbol theta
    "sigma": '\u03c3',      # σ, Σ, ς(final sigma)
}

PROTEIN_PATTERNS = [
    re.compile(r'\w*protein$', re.IGNORECASE),
    re.compile(r'\w*enzyme$', re.IGNORECASE),
    re.compile(r'\w*subunit$', re.IGNORECASE),
    re.compile(r'\w*isoform', re.IGNORECASE),
    re.compile(r'\w*peptdie', re.IGNORECASE),
    re.compile(r'\w*chain', re.IGNORECASE),
    re.compile(r'\w{3,}log$', re.IGNORECASE),
    re.compile(r'\w{3,}[oe]r$', re.IGNORECASE),
    re.compile(r'\w{3,}gen', re.IGNORECASE),
    re.compile(r'\w{3,}ase', re.IGNORECASE),
    re.compile(r'\w{3,}ant', re.IGNORECASE),
    re.compile(r'\w{2,}xin', re.IGNORECASE)
]


class Token:
    ALPHA2GREEK = ALPHA2GREEK
    PROTEIN_PATTERNS = PROTEIN_PATTERNS

    @classmethod
    def tokenize(cls, text):
        tokens = tokenize(text)[1]
        return tokens, cls.join(tokens)

    @classmethod
    def as_plural(cls, tokens):
        idx = cls.where_protein(tokens)
        res = list(tokens)
        res[idx] = pluralize(tokens[idx])
        return tuple(res), cls.join(res)

    @classmethod
    def as_greek(cls, tokens):
        res = tuple(cls.ALPHA2GREEK[token.lower()] if cls.is_greek(token) else token for token in tokens)
        return res, cls.join(res)

    @staticmethod
    def join(tokens):
        return unify(''.join(tokens))

    @classmethod
    def is_protein(cls, token):
        return any(pattern.fullmatch(token) for pattern in cls.PROTEIN_PATTERNS)

    @classmethod
    def is_greek(cls, token):
        return token.lower() in cls.ALPHA2GREEK

    @classmethod
    def where_protein(cls, tokens):
        """
        Return the last index of pluralizable protein n-gram.
        Returns -1 in the absence of such n-grams.
        """
        for i, token in enumerate(reversed(tokens), start=1):
            if cls.is_protein(token):
                return -i
        return -1


class KeyWord(str):
    RW_FILE = PathTemplate('$pdata/uniprot/uniprot_keywords.pkl').substitute()
    NAME_TAGS = NAME_TAGS
    INV_TAGS = INV_TAGS
    BLACK_LIST = BLACK_LIST
    __slots__ = ['type']

    def __new__(cls, kw_type, val):
        return super().__new__(cls, val)

    def __init__(self, kw_type, val):
        self.type = kw_type

    def type_tuple(self):
        return self.INV_TAGS[self.type]

    def get_all_alternatives(self):
        yield self.tokenize()
        if g := self.greek_exist():
            yield self.as_greek()
        if p := self.is_protein():
            yield self.as_plural()
        if g and p:
            yield self.as_greek_plural()

    def get_alts_for_assign(self):
        new = KeyWord(self.type, unify(self))
        return {tokens for tokens, _ in new.get_all_alternatives()}

    @classmethod
    def load(cls):
        """dict[KeyWord, set[Entry]]"""
        with open(cls.RW_FILE, 'rb') as file:
            return pickle.load(file)

    @classmethod
    def load_k2k(cls):
        return {k: k for k in cls.load()}

    def tokenize(self):
        return Token.tokenize(self)

    def as_plural(self):
        return Token.as_plural(self.tokenize()[0])

    def as_greek(self):
        return Token.as_greek(self.tokenize()[0])

    def as_greek_plural(self):
        return Token.as_plural(self.as_greek()[0])

    def __getnewargs__(self):
        return self.type, str(self)

    def is_protein(self):
        return self.type < 22  # Note that ('gene', *, *) starts with index 22

    def is_valid(self):
        return self.type not in self.BLACK_LIST

    def greek_exist(self):
        return any(Token.is_greek(token) for token in self.tokenize()[0])


@dataclass
class Reference:
    type: str
    pub_date: dict[str, int]
    pmid: int
    title: str = field(repr=False)
    scopes: list[str] = field(repr=False)

    def __repr__(self):
        if self.type == 'journal article':
            pub_date = ', '.join(f'{k}={v}' for k, v in self.pub_date.items())
            return f'https://pubmed.ncbi.nlm.nih.gov/{self.pmid}/ ({pub_date})'
        return f'Reference(type={self.type}, pub_date={self.pub_date})'


class EntryFinder:
    @classmethod
    def keyword(cls, *keys, strict=True):
        return cls._keyword_strict(*keys) if strict else cls._keyword_loose(*keys)

    @staticmethod
    def _keyword_strict(*keys):
        keys = set(keys)
        for e in Entry.values():
            _, ks = zip(*e._keywords)
            if not keys.isdisjoint(ks):
                yield e

    @staticmethod
    def _keyword_loose(*keys):
        keys = {key.lower() for key in keys}
        for e in Entry.values():
            if any(key.lower() in keys for _, key in e._keywords):
                yield e


class Entry(metaclass=MetaCacheExt):
    __slots__ = ['key_acc', 'name', 'accessions', '_keywords', '_references']
    CACHE_PATH = PathTemplate('$pdata/uniprot/uniprot_sprot_parsed.pkl.gz').substitute()
    finder = EntryFinder

    def __new__(cls, key_acc, caching=True):
        return super().__new__(cls)

    def __init__(self, key_acc, caching=True):
        self.key_acc = key_acc
        self.name: str = ''
        self.accessions: list[str] = []
        self._keywords: list[tuple[str, str]] = []
        self._references: list[dict] = []

    @property
    def first_pub_year(self):
        """
        return == 9999 indicates that the Entry has no valid publication yet
        """
        min_year = 9999
        for ref in self._references:
            match ref:
                case {'type': 'journal article' | 'online journal article',
                      'pub_date': {'Year': int(year)}}:
                    if year < min_year:
                        min_year = year
        return min_year

    @classmethod
    def from_parse(cls, dct):
        self = cls(dct['accessions'][0])
        self.name = dct['name']
        self.accessions = dct['accessions']
        self._keywords = [(KeyWord.NAME_TAGS[key_type], v) for (key_type, v) in chain(dct['proteins'], dct['genes'])]
        self._references = [x for x in dct['refs']]

    def __repr__(self):
        return f"Entry({self.name}, {self.key_acc})"

    @property
    def keywords(self):
        return [KeyWord(*x) for x in self._keywords]

    @property
    def references(self):
        return [Reference(**x) for x in self._references]

    def url(self):
        return f'https://uniprot.org/uniprotkb/{self.key_acc}/entry'

    @property
    def info(self):
        return dedent(f"""\
        {self}
                  Name: {self.name}
            Accessions: {_print_set(self.accessions)}
              Keywords: {_print_set(self.keywords)}
                   URL: {self.url()}
            References: {_print_refs(self.references)}
        """)

    def __getnewargs__(self):
        return self.key_acc, False


class Nested(dict, metaclass=MetaLoad):
    _R_FILE = PathTemplate('$pdata/uniprot/uniprot_keywords.pkl').substitute()
    LOAD_PATH = PathTemplate('$pdata/uniprot/nested.pkl').substitute()

    def match_and_filter(self, target_text):
        target_match_list = list(self.find_matches(target_text))
        target_match_list.sort(key=_sort_key)
        filter_smaller(target_match_list)
        return target_match_list

    def get_from_tuple(self, tuple_of_keys):
        cur = self
        for key in tuple_of_keys:
            cur = cur[key]
        return cur

    def in_from_tuple(self, tuple_of_keys):
        cur = self
        for key in tuple_of_keys:
            if key in cur:
                cur = cur[key]
            else:
                return False
        return True

    def deep_items(self):
        return deep_items(self)

    def find_matches(self, text):
        indices, tokens = tokenize(text)
        for i, (start, _) in enumerate(indices):  # i=token index, idx=location on target_text
            for matched_tokens, (entries, keywords) in self._matches(tokens[i:]):
                _, end = indices[i + len(matched_tokens) - 1]
                matched_text = text[start:end]
                yield Match(matched_tokens, entries, keywords, (start, end), matched_text)

    def _matches(self, tokens):
        i, cur = 0, self
        while i < len(tokens) and (lower := tokens[i].lower()) in cur:
            i += 1
            cur = cur[lower]
            if -1 in cur:
                matched = tokens[:i]
                if origins := cur[-1].get(Token.join(matched)):
                    yield matched, origins

    @classmethod
    def generate(cls):
        nested = super().__new__(cls)
        keywords: dict[KeyWord, list[str]] = KeyWord.load()
        for kw, key_accs in keywords.items():
            if kw.is_valid():
                for tokens, joined in kw.get_all_alternatives():
                    lower_tokens = tuple(token.lower() for token in tokens)
                    nested.extend(lower_tokens, joined, key_accs, str(kw))
        return nested

    def extend(self, lower_tokens, joined, key_accs, keywords):
        cur = self
        for lower_token in lower_tokens:
            cur = cur.setdefault(lower_token, {})
        val = cur.setdefault(-1, {}).setdefault(joined, [set(), set()])
        val[0].update(key_accs)
        val[1].add(keywords)


Match = namedtuple('Match', ['tokens', 'entries', 'keywords', 'spans', 'text'])


def _sort_key(match: Match):
    start, end = match.spans
    return start, start-end  # No typo here


def filter_smaller(matches: list[Match]):
    initial, final, i = 0, -1, 0
    while i < len(matches):
        ini, fin = matches[i].spans
        if (initial <= ini) and (fin <= final):
            matches.pop(i)
        else:
            initial, final = ini, fin
            i += 1


def deep_items(x, keys=()):
    for k, v in x.items():
        if k == -1:
            yield keys, v
        else:
            yield from deep_items(v, keys=(*keys, k))


def _print_set(s):
    if len(s) == 1:
        return str(next(iter(s)))
    elif s:
        return s
    else:
        return ''


def _print_refs(refs):
    if len(refs) == 1:
        return str(refs[0])
    elif refs:
        return ',  '.join(str(ref) for ref in refs)
    else:
        return ''
