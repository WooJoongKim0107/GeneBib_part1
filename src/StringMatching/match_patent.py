import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from UniProt.containers import Nested


R_FILE = PathTemplate('$pdata/patent/patent_$index.pkl.gz')
_R_FILE0 = PathTemplate('$pdata/uniprot/nested.pkl')
W_FILE = PathTemplate('$pdata/patent/matched/patent_$index.pkl.gz')

NESTED = Nested.load()  # Read0


def match_entire_file(index):
    with gzip.open(R_FILE.substitute(index=index), 'rb') as file:
        container = pickle.load(file)

    res = {}
    for pub, patent in container.items():
        title = NESTED.match_and_filter(patent.title)
        abstract = NESTED.match_and_filter(patent.abstract)
        if title or abstract:
            res[pub] = title, abstract

    with gzip.open(W_FILE.substitute(index=index), 'wb') as file:
        pickle.dump(res, file)
    return index


def main():
    with Pool(50) as p:
        for index in p.imap_unordered(match_entire_file, range(112)):
            print(f'{index}\n', end='')


if __name__ == '__main__':
    main()
