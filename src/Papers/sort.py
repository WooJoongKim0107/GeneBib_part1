import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal
from Papers.article_finder import ArticleFinder, JnlToPmids


_W_FILE = PathTemplate('$rsrc/pdata/paper/sorted/$journal.pkl.gz')


def write(journal: Journal, pmids):
    if journal.match_path.is_file():
        print(journal.medline_ta, ': already done')
        return

    key = journal.medline_ta
    with gzip.open(journal.match_path, 'wb') as file:
        pickle.dump(journal.counts+1, file)
        pickle.dump(key, file)
        for article in ArticleFinder.from_pmids(*pmids):
            pickle.dump(article, file)
    print(journal._simple_title)


def main():
    j2p = JnlToPmids()
    args = [(journal, j2p[key]) for key, journal in Journal.unique_items()]
    del j2p

    with Pool(5) as p:
        p.starmap(write, args)


if __name__ == '__main__':
    main()
