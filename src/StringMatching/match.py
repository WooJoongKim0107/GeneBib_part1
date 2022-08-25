import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal
from UniProt.containers import Nested


NESTED = Nested(True)

R_FILE = PathTemplate('$base/journals_selected.pkl')
W_FILE = PathTemplate('$rsrc/pdata/paper/matched/$journal.pkl.gz')


def match_entire_journal(medline_ta):
    res = {}
    journal = Journal[medline_ta]
    for art in journal.get_articles():
        title = NESTED.match_and_filter(art.title)
        abstract = NESTED.match_and_filter(art.abstract)
        if title or abstract:
            res[art.pmid] = title, abstract

    with gzip.open(W_FILE.substitute(journal=journal._simple_title), 'wb') as file:
        pickle.dump(res, file)
    print(medline_ta)


def main():
    with Pool(50) as p:
        p.map(match_entire_journal, Journal.unique_keys())


if __name__ == '__main__':
    main()
