import re
import pickle
from functools import cache
from numpy import binary_repr
from StringMatching.base import plural_keyw, uniform_match, get_ngram_list3


alph2greek_dict = {
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
    "sigma": '\u03c3',       # σ, Σ, ς(final sigma)
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

@cache
def protein_keyword(kw_type):
    return binary_repr(kw_type, width=8).startswith('0')

def is_protein(ngram):
    return any(pattern.fullmatch(ngram) for pattern in protein_patterns)

def protein_idx(ngrams):
    """
    Return the last index of pluralizable protein n-gram.
    Returns -1 in the absence of such n-grams.
    """
    for i, ngram in enumerate(reversed(ngrams), start=1):
        if is_protein(ngram):
            return -i
    return -1

def pluralize(ngrams):
    idx = protein_idx(ngrams)
    val = plural_keyw(ngrams[idx])
    res = list(ngrams)
    res[idx] = val
    return tuple(res)

def isgreek(ngram):
    return ngram.lower() in alph2greek_dict

def as_greek(ngrams):
    return [alph2greek_dict[ngram.lower()] if isgreek(ngram) else ngram for ngram in ngrams]

def indexing(ngrams):
    return tuple(unip[ngram] for ngram in ngrams)

def append_unip(x):
    return unip.setdefault(x, len(unip))

def update_unip(ngrams):
    for ngram in ngrams:
        append_unip(ngram)
        append_unip(ngram.lower())

def update_nested(indices):
    level = nested
    for idx in indices:
        level = level.setdefault(idx, {})
    level[-1] = 0

def update_unif(indices, concat):
    unif.setdefault(indices, set()).add(concat)

def update_nested_and_unif(x):
    indices = indexing(v.lower() for v in x)
    concat = uniform_match(''.join(x))
    update_nested(indices)
    update_unif(indices, concat)


with open('./keyword_list_200201.pickle', 'rb') as uniprot_keyw_file:
    uniprot_keyw_list = pickle.load(uniprot_keyw_file)
print(f"uniprot_keyw_list : {len(uniprot_keyw_list)}")
unip = {}  # {possible ngram -> idx}
unif = {}  # {idx path -> joined ngrams}
nested = {}  # idx path dictionary
for kw, kw_type in uniprot_keyw_list:
    ngram_list, _ = get_ngram_list3(kw)
    update_unip(ngram_list)
    update_nested_and_unif(ngram_list)

    greek = as_greek(ngram_list)
    update_unip(greek)
    update_nested_and_unif(greek)

    if protein_keyword(kw_type):
        plural = pluralize(ngram_list)
        update_unip(plural)
        update_nested_and_unif(plural)

        gp = pluralize(greek)
        update_unip(gp)
        update_nested_and_unif(gp)

print(len(unip), ": unip")
print(len(unif), ": unif")
print(len(nested), ": nested")
print("UniProt gram corpus loaded")

# uniprot_keyw_list : 934232
# 332771 : unip
# 1170704 : unif
# 69211 : nested
# UniProt gram corpus loaded
