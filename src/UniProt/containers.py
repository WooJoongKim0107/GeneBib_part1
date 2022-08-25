import re
import pickle
from textwrap import dedent
from itertools import chain
from collections import namedtuple
from dataclasses import dataclass, field
from more_itertools import nth
from mypathlib import PathTemplate
from StringMatching.base import plural_keyw, get_ngram_list3, uniform_match

# name_tags provides maps to old-style kw_type
name_tags = {
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
    ('gene', 'name', 'ORF'): 25  # Not used
}
inv_tags = {v: k for k, v in name_tags.items()}
deprecated = {2, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 24, 25}

alpha2greek = {
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

protein_patterns = [
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
    ALPHA2GREEK = alpha2greek
    PROTEIN_PATTERNS = protein_patterns

    @classmethod
    def tokenize(cls, text):
        tokens = get_ngram_list3(text)[1]
        return tokens, cls.join(tokens)

    @classmethod
    def as_plural(cls, tokens):
        idx = cls.where_protein(tokens)
        res = list(tokens)
        res[idx] = plural_keyw(tokens[idx])
        return tuple(res), cls.join(res)

    @classmethod
    def as_greek(cls, tokens):
        res = tuple(cls.ALPHA2GREEK[token.lower()] if cls.is_greek(token) else token for token in tokens)
        return res, cls.join(res)

    @staticmethod
    def join(tokens):
        return uniform_match(''.join(tokens))

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
    NAME_TAGS = name_tags
    INV_TAGS = inv_tags
    DEPRECATED = deprecated
    __slots__ = ['type']

    def __new__(cls, kw_type, val):
        return super().__new__(cls, val)

    def __init__(self, kw_type, val):
        self.type = kw_type

    def type_tuple(self):
        return self.INV_TAGS[self.type]

    def get_all_tokens(self):
        match self.greek_exist(), self.is_protein():
            case False, False:
                return [self.tokenize()]
            case False, True:
                return [self.tokenize(), self.as_plural()]
            case True, False:
                return [self.tokenize(), self.as_greek()]
            case True, True:
                return [self.tokenize(), self.as_greek(), self.as_plural(), self.as_greek_plural()]

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
        return self.type not in self.DEPRECATED

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
        else:
            return f'Reference(type={self.type}, pub_date={self.pub_date})'


class UniProtEntry:
    __slots__ = ['name', 'accessions', '_keywords', '_references']

    def __init__(self, dct):
        self.name = dct['name']
        self.accessions = dct['accessions']
        self._keywords = [(KeyWord.NAME_TAGS[key_type], v) for (key_type, v) in chain(dct['proteins'], dct['genes'])]
        self._references = [x for x in dct['refs']]

    def __repr__(self):
        return f"UniProtEntry({self.name}, {self.key_acc})"

    @property
    def key_acc(self):
        return self.accessions[0]

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


class Nested(dict):
    R_FILE = PathTemplate('$base/uniprot_keywords.pkl')
    W_FILE = PathTemplate('$base/nested.pkl')

    def __init__(self, load=True):
        if load:
            data = self.load()
        else:
            data = self.generate()
        super().__init__(data)

    def extend(self, accs, lower_tokens, value):
        cur = self
        for lower_token in lower_tokens:
            cur = cur.setdefault(lower_token, {})
        cur.setdefault(-1, {}).setdefault(value, []).extend(accs)

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

    def strict_matches(self, text):
        indices, tokens = get_ngram_list3(text)
        for i, (start, _) in enumerate(indices):  # i=token index, idx=location on target_text
            for accs, matched_tokens in self._strict_matches(tokens[i:]):
                _, end = indices[i + len(matched_tokens) - 1]
                matched_text = text[start:end]
                yield Match(accs, matched_tokens, (start, end), matched_text)

    def _matches(self, lower_tokens):
        i = 0
        cur = self
        while i < len(lower_tokens) and (lower_token := lower_tokens[i]) in cur:
            i += 1
            cur = cur[lower_token]
            if -1 in cur:
                yield cur[-1], lower_tokens[:i]

    def _strict_matches(self, tokens):
        for joined2accs, matched_tokens in self._matches(tuple(token.lower() for token in tokens)):
            raw_joined = uniform_match(''.join(tokens[:len(matched_tokens)]))
            if raw_joined in joined2accs:
                yield joined2accs[raw_joined], matched_tokens

    def strict_matches2(self, text):
        """
        BAG-1L -> (BAG-1 + 1L) matches on older versions
        BAG-1L -> (BAG-1 + L) matches on this version.
        Thus, keep using strict_matches() rather than this.
        """
        indices, tokens = get_ngram_list3(text)
        lower_tokens = tuple(token.lower() for token in tokens)

        i = 0
        while i < len(tokens):
            if lower_tokens[i] in self:
                cur = self
                j = i
                accs = []
                terminal = 0

                while True:
                    cur = cur[lower_tokens[j]]
                    j += 1
                    if -1 in cur:
                        raw_joined = uniform_match(''.join(tokens[i:j]))
                        if raw_joined in cur[-1]:
                            accs = cur[-1][raw_joined]
                            terminal = j
                    if not (j < len(tokens) and lower_tokens[j] in cur):
                        break

                if accs:
                    (start, _), (_, end) = indices[i], indices[terminal-1]
                    yield Match(accs, lower_tokens[i:terminal], (start, end), text[start:end])
                    i = terminal
                else:
                    i += 1
            else:
                i += 1

    @classmethod
    def generate(cls):
        with open(cls.R_FILE.substitute(), 'rb') as file:
            keywords: dict[KeyWord, list[str]] = pickle.load(file)

        nested = super().__new__(cls)
        for kw, accs in keywords.items():
            if kw.is_valid():
                for tokens, joined in kw.get_all_tokens():
                    lower_tokens = tuple(token.lower() for token in tokens)
                    nested.extend(accs, lower_tokens, joined)
        return nested

    @classmethod
    def load(cls):
        with open(cls.W_FILE.substitute(), 'rb') as file:
            return pickle.load(file)

    def dump(self):
        with open(self.W_FILE.substitute(), 'wb') as file:
            pickle.dump(dict(self), file)


def deep_items(x, keys=()):
    for k, v in x.items():
        if k == -1:
            yield keys, v
        else:
            yield from deep_items(v, keys=(*keys, k))


Match = namedtuple('Match', ['accessions', 'tokens', 'spans', 'text'])


class Unip(dict):
    """
    {lower_token -> index; for all tokens from UniProtKB entries}
    Index is an integer assigned to each token in insertion order.

    This class is not used anymore, but left for future debugging.
    """
    def append(self, lower_token):
        return self.setdefault(lower_token, len(self))

    def extend(self, lower_tokens):
        for lower_token in lower_tokens:
            self.append(lower_token)

    def find(self, index):
        return nth(self.values(), index)


class Unif(dict):
    """
    {index chain -> joined tokens; for all chains from UniProtKB entries}

    This class is not used anymore, but left for future debugging.
    """
    def extend(self, lower_tokens, value):
        self.setdefault(lower_tokens, set()).add(value)


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
