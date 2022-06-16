import gzip, pickle, tarfile
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal
from Papers.article_finder import ArticleFinder, JnlToPmids


_W_FILE0 = PathTemplate('$rsrc/pdata/paper/papers.tar')
_W_FILE1 = PathTemplate('$rsrc/pdata/paper/sorted/$journal.pkl.gz')
assert str(_W_FILE0.substitute()) == str(Journal._TAR_PATH)
assert str(_W_FILE1) == str(Journal._ARTICLE_PATH)


def write(journal: Journal, pmids):
    if journal.path.is_file():
        print(journal.medline_ta, ': already done')
        return

    key = journal.medline_ta
    with gzip.open(journal.path, 'wb') as file:
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
