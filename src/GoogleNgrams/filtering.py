import gzip
import pickle
from collections import ChainMap
from mypathlib import PathTemplate
from GoogleNgrams.as_dict import W_FILES as _R_FILES

_R_FILE = PathTemplate('/home/data/01_data/google_ngrams/1grams/1grams.pkl.gz', key='{:0>5}'.format).substitute()
R_FILES = {'ngrams': PathTemplate('$base/ngram_curation/1903182220KPO.${n}gramOnSort'),
           'nonME': PathTemplate('$base/ngram_curation/1908272135ZKO.ngramSorted_nonME').substitute(),
           'ngram_counts': _R_FILES['total'],
           'path': PathTemplate('$base/matched_paths.pkl').substitute(),
           'counts': PathTemplate('$base/match_counts.pkl').substitute(),
           'answered': PathTemplate('$base/papat_hit_phrase_200517.pkl').substitute()}
W_FILE = PathTemplate('$base/${n}grams4filter.txt')


def get_answered():
    with open(R_FILES['answered'], 'rb') as file:
        answered = set(pickle.load(file))
    ci_preserved_keys = {}
    ci_removed_keys = {}
    for i in range(1, 7):
        with open(R_FILES['ngrams'].substitute(n=i), 'rt') as file:
            for line in file.read().splitlines()[1:]:
                removed, key, num_paper, paper_hit, ngram_hit = line.split('; ')[:-1]
                removed = int(removed)
                # ngram_hit = int(ngram_hit)
                answered.add(key)
                if removed:
                    ci_removed_keys.setdefault(key.lower(), set()).add(key)
                else:
                    ci_preserved_keys.setdefault(key.lower(), set()).add(key)

    with open(R_FILES['nonME'], 'rt') as file:
        for line in file.read().splitlines()[1:]:
            removed, key, key1, other, ngram_hit = line.split('; ')[:-1]
            removed = int(removed)
            # ngram_hit = int(ngram_hit)
            answered.add(key)
            if removed:
                ci_removed_keys.setdefault(key.lower(), set()).add(key)
            else:
                ci_preserved_keys.setdefault(key.lower(), set()).add(key)
    return answered, ci_preserved_keys, ci_removed_keys


def get_todo():
    with open(R_FILES['path'], 'rb') as file:
        paths = pickle.load(file)

    keys = {}
    for key, path in paths.items():
        keys.setdefault(len(path), set()).add(key)

    with open(R_FILES['counts'], 'rb') as file:
        counts = pickle.load(file)
    return keys, counts


def list_up():
    todo, todo_counts = get_todo()
    answered, preserved, removed = get_answered()

    already = answered.copy()
    keys_to_check = {}
    for n, keys in todo.items():
        for key in keys.difference(answered):
            if len(key) <= 1:
                already.add(key)
            elif key.lower() in removed:
                if any(check_upper(key) <= check_upper(v) for v in removed[key.lower()]):
                    already.add(key)
                else:
                    keys_to_check.setdefault(n, set()).add(key)
            elif key.lower() in preserved:
                if any(check_upper(key) >= check_upper(v) for v in preserved[key.lower()]):
                    already.add(key)
                else:
                    keys_to_check.setdefault(n, set()).add(key)
            else:
                keys_to_check.setdefault(n, set()).add(key)

    with gzip.open(R_FILES['ngram_counts'], 'rb') as file:
        ngram_counts = pickle.load(file)

    res = {}
    ci_answered = ChainMap(preserved, removed)
    for n, keys in keys_to_check.items():
        temp = []
        for key in keys:
            tc = todo_counts[key]
            nc = ngram_counts.get(key, 0)
            if key.lower() in removed:
                where = 'removed'
            elif key.lower() in preserved:
                where = 'preserved'
            elif key.lower() in answered:
                where = 'answered'
            else:
                where = 'undetermined'
            alias = ci_answered.get(key.lower(), '')
            temp.append((key, tc, nc, where, alias))
        res[n] = sorted(temp, key=lambda x: (len(x[-1]) > 0, *x[::-1][1:]), reverse=True)

    return res, already


def check_upper(s: str):
    if s.isupper():
        return 2  # 'UPPER'
    elif s[0].isupper() and not all(v.islower() for v in s[1:]):
        return 3  # 'ATPase'
    elif s[0].isupper() and all(v.islower() for v in s[1:]):
        return 1  # 'Upper'
    elif s.islower():
        return 0  # 'lower'
    else:
        return 1  # 'cAMP'


if __name__ == '__main__':
    q, w = list_up()
    for n, x in q.items():
        with open(W_FILE.substitute(n=n), 'wt') as file:
            file.write('keyword, paper_matches, ngram_counts, case_insensitive_in_removed\n')
            for k, pm, nc, wh, al in x:
                print(k, pm, nc, wh, al, sep=', ', end='\n', file=file)
