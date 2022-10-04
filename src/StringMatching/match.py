import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal  # Read0
from UniProt.containers import Nested


_R_FILE0 = PathTemplate('$rsrc/pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$rsrc/pdata/uniprot/nested.pkl')
_R_FILE2 = PathTemplate('$rsrc/pdata/paper/sorted/$journal.pkl.gz')
_W_FILE = PathTemplate('$rsrc/pdata/paper/matched/$journal.pkl.gz')

NESTED = Nested.load()  # Read1


def match_entire_journal(medline_ta):
    res = {}
    journal = Journal(medline_ta)
    for art in journal.get_articles():  # Read2
        title = NESTED.match_and_filter(art.title)
        abstract = NESTED.match_and_filter(art.abstract)
        if title or abstract:
            res[art.pmid] = title, abstract

    with gzip.open(journal.match_path, 'wb') as file:
        pickle.dump(res, file)
    return medline_ta


def main():
    with Pool(50) as p:
        for medline_ta in p.imap_unordered(match_entire_journal, Journal.unique_keys()):
            print(medline_ta+'\n', end='')


if __name__ == '__main__':
    main()
