import pickle
from itertools import chain
from collections import Counter
from multiprocessing import Pool
from Papers import Journal
from mypathlib import PathTemplate


W_FILES = {'counts': PathTemplate('$base/match_counts.pkl').substitute(),
           'paths': PathTemplate('$base/matched_paths.pkl').substitute()}


def get_matched_texts(j):
    res = {}
    c = Counter()
    for matches in j.get_matches().values():
        for match in chain(*matches):
            c[match.text] += 1
            res[match.text] = match.tokens
    print(j.medline_ta)
    return c, res


def main(js):
    _counts = Counter()
    _paths = {}
    for j in js:
        _count, _path = get_matched_texts(j)
        _counts += _count
        _paths.update(_path)
    return _counts, _paths


if __name__ == '__main__':
    counts = Counter()
    paths = {}
    # args = Journal.journals4mp(50)
    # print(len(Journal.unique_keys()))
    # assert sum(map(len, args)) == len(Journal.unique_keys())
    # assert set(j for js in args for j in js) == set(Journal.unique_values())

    with Pool(50) as p:
        # for count, path in p.map(main, args):
        for count, path in p.map(get_matched_texts, Journal.unique_values()):
            counts += count
            paths.update(path)

    with open(W_FILES['counts'], 'wb') as file:
        pickle.dump(counts, file)
    with open(W_FILES['paths'], 'wb') as file:
        pickle.dump(paths, file)
