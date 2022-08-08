import re


greek_dict = {
    'α': "alpha",
    'β': "beta",
    'γ': "gamma",
    'δ': "delta",
    'ε': "epsilon",
    'ζ': "zeta",
    'η': "eta",
    'θ': "theta",
    'ι': "iota",
    'κ': "kappa",
    'λ': "lamda",
    'μ': "mu",
    'ν': "nu",
    'ξ': "xi",
    'ο': "omicron",
    'π': "pi",
    'ρ': "rho",
    'ς': "final",
    'σ': "sigma",
    'τ': "tau",
    'υ': "upsilon",
    'φ': "phi",
    'χ': "chi",
    'ψ': "psi",
    'ω': "omega",

    'Α': "alpha",
    'Β': "beta",
    'Γ': "gamma",
    'Δ': "delta",
    'Ε': "epsilon",
    'Ζ': "zeta",
    'Η': "eta",
    'Θ': "theta",
    'Ι': "iota",
    'Κ': "kappa",
    'Λ': "lamda",
    'Μ': "mu",
    'Ν': "nu",
    'Ξ': "xi",
    'Ο': "omicron",
    'Π': "pi",
    'Ρ': "rho",
    'Σ': "sigma",
    'Τ': "tau",
    'Υ': "upsilon",
    'Φ': "phi",
    'Χ': "chi",
    'Ψ': "psi",
    'Ω': "omega",
    'ϴ': "theta"
}
ngram_pattern = re.compile(r'\d+(\.\d+)+|\$\d+(\.\d+)?|[a-zA-Z0-9]+\++|[a-zA-Z_]+|\d+|[^a-zA-Z0-9_ ]')

def uniform_match(aKeyword) :
    keywLength = len(aKeyword)
    if keywLength > 4:
        unifiedKeyw = aKeyword.lower()
    elif keywLength > 2:
        unifiedKeyw = aKeyword[0].upper()+aKeyword[1:]
    else :
        unifiedKeyw = aKeyword
    return unifiedKeyw


def plural_keyw(text=''):
    postfix = 's'
    if len(text) > 2:
        if text[-2:] in ('ch', 'sh', 'ss'):
            postfix = 'es'
        elif text[-1:] in ('s', 'x', 'z'):
            postfix = 'es'
    return text + postfix


def get_ngram_list3(fullPhrase):
    if not fullPhrase.strip():  # strip() only used to check if empty
        return (), ()
    finditer = ngram_pattern.finditer(fullPhrase)
    start_phrase = iter([match.start(), match.group()] for match in finditer)
    indices, tokens = zip(*start_phrase)
    return indices, tokens
