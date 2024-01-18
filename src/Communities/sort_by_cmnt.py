import gzip
import pickle
from multiprocessing import Pool
from functools import partial
from myclass import TarRW
from mypathlib import PathTemplate
from Papers import Article
from UsPatents import UsPatent
from CnPatents import CnPatent
from EpPatents import EpPatent


_R_FILE0s = {'paper': PathTemplate('$pdata/paper/paper_$index.pkl.gz'),
             'us_patent': PathTemplate('$pdata/us_patent/patent_$index.pkl.gz'),
             'cn_patent': PathTemplate('$pdata/cn_patent/patent_$index.pkl.gz'),
             'ep_patent': PathTemplate('$pdata/ep_patent/patent_$index.pkl.gz'),}
R_FILE1s = {'paper': PathTemplate('$lite/paper/pmid2cmnt.pkl').substitute(),
            'us_patent': PathTemplate('$lite/us_patent/pubnum2cmnt.pkl').substitute(),
            'cn_patent': PathTemplate('$lite/cn_patent/pubnum2cmnt.pkl').substitute(),
            'ep_patent': PathTemplate('$lite/ep_patent/pubnum2cmnt.pkl').substitute(),}

W_FILE0s = {'paper': PathTemplate('$pdata/paper/sorted/${cmnt_idx}.pkl.gz'),
            'us_patent': PathTemplate('$pdata/us_patent/sorted/${cmnt_idx}.pkl.gz'),
            'cn_patent': PathTemplate('$pdata/cn_patent/sorted/${cmnt_idx}.pkl.gz'),
            'ep_patent': PathTemplate('$pdata/ep_patent/sorted/${cmnt_idx}.pkl.gz'),}
W_FILE1s = {'paper': PathTemplate('$pdata/paper/sorted/community.tar').substitute(),
            'us_patent': PathTemplate('$pdata/us_patent/sorted/community.tar').substitute(),
            'cn_patent': PathTemplate('$pdata/cn_patent/sorted/community.tar').substitute(),
            'ep_patent': PathTemplate('$pdata/ep_patent/sorted/community.tar').substitute(),}


def foo(mode, pmid2cmnt, index):
    if mode == 'paper':
        cls = Article
    elif mode == 'us_patent':
        cls = UsPatent
    elif mode == 'cn_patent':
        cls = CnPatent
    elif mode == 'ep_patent':
        cls = EpPatent
    else:
        raise ValueError

    res = {}
    for pmid, article in cls.load(index).items():  # R0['cls']
        for cmnt in pmid2cmnt.get(pmid, set()):
            res.setdefault(cmnt, {})[pmid] = article
    return res


def merge(results):
    anc = results[0].copy()
    for res in results[1:]:
        for cmnt, dct in res.items():
            anc.setdefault(cmnt, {}).update(dct)
    return sort(anc)


def sort(klv):
    x = {}
    for k in sorted(klv):
        x[k] = {}
        for l in sorted(klv[k]):
            x[k][l] = klv[k][l]
    return x


def write(mode, cmnt_idx, articles):
    fn = W_FILE0s[mode].substitute(cmnt_idx=cmnt_idx)
    with gzip.open(fn, 'wb') as file:
        pickle.dump(articles, file)
    return fn


def _main(mode):
    with open(R_FILE1s[mode], 'rb') as file:
        pmid2cmnt = pickle.load(file)
    f = partial(foo, mode, pmid2cmnt)

    with Pool(8) as p:
        results = p.map(f, range(112))
    result = merge(results)

    with Pool(8) as p:
        paths = p.starmap(partial(write, mode), result.items())

    with TarRW(W_FILE1s[mode], 'w') as q:
        q.extend(paths)


def main():
    for mode in ['paper', 'us_patent', 'cn_patent', 'ep_patent']:
        _main(mode)


if __name__ == '__main__':
    main()
