import pickle
from itertools import chain
from collections import Counter
from multiprocessing import Pool
from Papers import Journal  # Read0
from mypathlib import PathTemplate


_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$pdata/paper/matched/$journal.pkl.gz')
W_FILES = {'counts': PathTemplate('$lite/match/match_counts.pkl').substitute(),
           'paths': PathTemplate('$lite/match/matched_paths.pkl').substitute(),
           'parents': PathTemplate('$lite/match/matched_parents.pkl').substitute(),
           'double_check': PathTemplate('$lite/match/matched_double_check.pkl').substitute()}


def get_matched_texts(j):
    res = {}
    c = Counter()
    parents = {}
    for matches in j.get_matches().values():  # Read1
        for match in chain(*matches):
            c[match.text] += 1
            res[match.text] = match.tokens
            parents[match.text] = match.entries, match.keywords
    return c, res, parents, j


def _main(js):
    _counts = Counter()
    _paths = {}
    _parents = {}
    _double_check = []
    for j in js:
        _count, _path, _parent, _j = get_matched_texts(j)
        _counts += _count
        _paths.update(_path)
        _parents.update(_parent)
        _double_check.append(_j)
    return _counts, _paths, _parents, _double_check


def main():
    counts = Counter()
    paths = {}
    parents = {}
    args = Journal.journals4mp(50, selected=False)

    double_check = []
    with Pool(50) as p:
        for count, path, parent, check in p.imap_unordered(_main, args):
            counts += count
            paths.update(path)
            parents.update(parent)
            double_check.append(check)

    with open(W_FILES['counts'], 'wb') as file:
        pickle.dump(counts, file)
    with open(W_FILES['paths'], 'wb') as file:
        pickle.dump(paths, file)
    with open(W_FILES['parents'], 'wb') as file:
        pickle.dump(parents, file)
    with open(W_FILES['double_check'], 'wb') as file:
        pickle.dump(double_check, file)


if __name__ == '__main__':
    main()
