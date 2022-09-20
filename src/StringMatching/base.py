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


def uniform_keyw_2(x):
    """Used when localizing keywords after clustering UniProtEntries"""
    keywLength = len(x)
    outputKeyw = x
    outputKeyw = re.sub(' ?, ?', ',', outputKeyw)
    outputKeyw = re.sub('\[', '(', outputKeyw)
    outputKeyw = re.sub('\]', ')', outputKeyw)
    outputKeyw = re.sub('(?i)^probable ', '', outputKeyw)
    outputKeyw = re.sub('(?i)^putative ', '', outputKeyw)
    outputKeyw = re.sub('(?i) homolog(,? \w+)?$', '', outputKeyw)
    outputKeyw = re.sub('(?i) isoform(,? \w+)?$', '', outputKeyw)
    if keywLength > 4:
        outputKeyw = outputKeyw.lower()
    elif keywLength > 2:
        outputKeyw = outputKeyw[0].upper()+outputKeyw[1:]
    return outputKeyw
