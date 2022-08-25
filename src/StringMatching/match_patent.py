import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from StringMatching.match import match_and_filter
from Patents.merge import W_FILE as R_FILE


W_FILE = PathTemplate('$rsrc/pdata/patent/matched/patent_$index.pkl.gz')


def main(index):
    with gzip.open(R_FILE.substitute(index=index), 'rb') as file:
        container = pickle.load(file)

    res = {}
    for pub, patent in container.items():
        title = match_and_filter(patent.title)
        abstract = match_and_filter(patent.abstract)
        if title or abstract:
            res[pub] = title, abstract

    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)
    print(index)


if __name__ == '__main__':
    todo = range(112)
    with Pool(50) as p:
        p.map(main, todo)
