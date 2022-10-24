import gzip
import pickle
from multiprocessing import Pool
from functools import partial
from myclass import TarRW
from mypathlib import PathTemplate


R_FILE0s = {'paper': PathTemplate('$pdata/paper/paper_$index.pkl.gz'),
            'patent': PathTemplate('$pdata/patent/patent_$index.pkl.gz')}
R_FILE1s = {'paper': PathTemplate('$lite/paper/pmid2cmnt.pkl').substitute(),
            'patent': PathTemplate('$lite/patent/pubnum2cmnt.pkl').substitute()}

W_FILE0s = {'paper': PathTemplate('$pdata/paper/sorted/${cmnt_idx}.pkl.gz'),
            'patent': PathTemplate('$pdata/patent/sorted/${cmnt_idx}.pkl.gz')}
W_FILE1s = {'paper': PathTemplate('$pdata/paper/sorted/community.tar').substitute(),
            'patent': PathTemplate('$pdata/patent/sorted/community.tar').substitute()}


def foo(mode, pmid2cmnt, index):
    with gzip.open(R_FILE0s[mode].substitute(index=index), 'rb') as file:
        chain_map = pickle.load(file)

    res = {}
    for pmid, article in chain_map.items():
        for cmnt in pmid2cmnt.get(pmid, set()):
            res.setdefault(cmnt, []).append(article)
    return res


def merge(results):
    anc = results[0].copy()
    for res in results[1:]:
        for cmnt, arts in res.items():
            anc.setdefault(cmnt, []).extend(arts)
    return anc


def write(mode, cmnt_idx, articles):
    fn = W_FILE0s[mode].substitute(cmnt_idx=cmnt_idx)
    with gzip.open(fn, 'wb') as file:
        pickle.dump(articles, file)
    return fn


def _main(mode):
    with open(R_FILE1s[mode], 'rb') as file:
        pmid2cmnt = pickle.load(file)
    args = [(pmid2cmnt, index) for index in range(112)]

    with Pool(50) as p:
        results = p.starmap(partial(foo, mode), args)
    result = merge(results)

    with Pool(50) as p:
        paths = p.starmap(partial(write, mode), result.items())

    with TarRW(W_FILE1s[mode], 'w') as q:
        q.extend(paths)


def main():
    for mode in ['paper', 'patent']:
        _main(mode)
