import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from myclass.tar import TarWrite

R_FILE0 = PathTemplate('$rsrc/pdata/paper/paper_$index.pkl.gz')
R_FILE1 = PathTemplate('$rsrc/lite/paper/pmid2cmnt.pkl').substitute()
W_FILE0 = PathTemplate('$rsrc/pdata/paper/sorted/${cmnt_idx}.pkl')
W_FILE1 = PathTemplate('$rsrc/pdata/paper/sorted/community.tar.gz').substitute()


def foo(pmid2cmnt, index):
    with gzip.open(R_FILE0.substitute(index=index), 'rb') as file:
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


def write(cmnt_idx, articles):
    fn = W_FILE0.substitute(cmnt_idx=cmnt_idx)
    with open(fn, 'wb') as file:
        pickle.dump(articles, file)
    return fn


def main():
    with open(R_FILE1, 'rb') as file:
        pmid2cmnt = pickle.load(file)
    args = [(pmid2cmnt, index) for index in range(112)]

    with Pool(50) as p:
        results = p.starmap(foo, args)
    result = merge(results)

    with Pool(50) as p:
        paths = p.starmap(write, result.items())

    with TarWrite(W_FILE1, 'w:gz') as q:
        for path in paths:
            q.add(path)

