import gzip
import pickle
from multiprocessing import Pool
from myclass import TarRW
from mypathlib import PathTemplate
from Papers import Journal  # Read0
from UniProt.containers import Nested


_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$pdata/uniprot/nested.pkl')
_R_FILE2 = PathTemplate('$pdata/paper/sorted/$journal.pkl.gz')
_W_FILE = PathTemplate('$pdata/paper/matched/$journal.pkl.gz')
W_FILE = PathTemplate('$pdata/paper/matched/journal.tar').substitute()

NESTED = Nested.load()  # Read1


def match_entire_journal(journal):
    res = {}
    for art in journal.get_articles():  # Read2
        title = NESTED.match_and_filter(art.title)
        abstract = NESTED.match_and_filter(art.abstract)
        if title or abstract:
            res[art.pmid] = title, abstract

    with gzip.open(journal.match_path, 'wb') as file:
        pickle.dump(res, file)
    return journal


def main():
    paths = []
    with Pool(50) as p:
        for j in p.imap_unordered(match_entire_journal, Journal.unique_values()):
            print(j.medline_ta+'\n', end='')
            paths.append(j.match_path)

    with TarRW(W_FILE, 'w') as tf:
        tf.extend(paths)


if __name__ == '__main__':
    main()
