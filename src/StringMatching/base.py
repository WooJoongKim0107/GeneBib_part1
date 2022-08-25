import re

ngram_pattern = re.compile(r'\d+(\.\d+)+|\$\d+(\.\d+)?|[a-zA-Z0-9]+\++|[a-zA-Z_]+|\d+|[^a-zA-Z0-9_ ]')


def unify(s: str):
    if len(s) > 4:
        return s.lower()
    elif len(s) > 2:
        return s[0].upper()+s[1:]
    else:
        return s


def pluralize(s: str):
    if len(s) > 2:
        if s[-2:] in ('ch', 'sh', 'ss') or s[-1:] in ('s', 'x', 'z'):
            return s + 'es'
    return s + 's'


def tokenize(s: str):
    if not s.strip():  # strip() only used to check if empty
        return (), ()
    finditer = ngram_pattern.finditer(s)
    span_phrase = iter([match.span(), match.group()] for match in finditer)
    indices, tokens = zip(*span_phrase)
    return indices, tokens
