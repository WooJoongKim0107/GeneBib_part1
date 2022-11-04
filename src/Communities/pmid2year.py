import gzip
import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool
from functools import partial


R_FILES = {'paper': PathTemplate('$pdata/paper/paper_$index.pkl.gz'),
           'patent': PathTemplate('$pdata/patent/patent_$index.pkl.gz')}
W_FILES = {'paper': PathTemplate('$lite/paper/pmid2year.pkl').substitute(),
           'patent': PathTemplate('$lite/patent/pubnum2year.pkl').substitute()}


def do(mode, index):
    with gzip.open(R_FILES[mode].substitute(index=index), 'rb') as file:
        chain = pickle.load(file)

    res = {}
    for pmid, art in chain.items():
        if 'Year' in art.pub_date:
            res[pmid] = art.pub_date['Year']
    return res


def _main(mode: str):
    pmid2year = {}
    func = partial(do, mode)
    with Pool(10) as p:
        for res in p.imap_unordered(func, range(112)):
            pmid2year.update(res)

    with open(W_FILES[mode], 'wb') as file:
        pickle.dump(pmid2year, file)


def main():
    for mode in ['paper', 'patent']:
        _main(mode)


if __name__ == '__main__':
    main()
