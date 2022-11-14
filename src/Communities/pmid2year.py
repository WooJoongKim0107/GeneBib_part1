import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool
from functools import partial
from Papers import Article
from Patents import Patent


_R_FILES = {'paper': PathTemplate('$pdata/paper/paper_$index.pkl.gz'),
            'patent': PathTemplate('$pdata/patent/patent_$index.pkl.gz')}
W_FILES = {'paper': PathTemplate('$lite/paper/pmid2year.pkl').substitute(),
           'patent': PathTemplate('$lite/patent/pubnum2year.pkl').substitute()}


def do(mode, index):
    cls, attr = {'paper': (Article, 'pub_date'),
                 'patent': (Patent, 'filing_date')}[mode]

    res = {}
    for pmid, art in cls.load(index).items():
        if year := getattr(art, attr).get('Year'):
            res[pmid] = year
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
