import gzip, pickle, tarfile
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal
from Papers.article_finder import ArticleFinder


_W_FILE0 = PathTemplate('$rsrc/pdata/paper/papers.tar.gz')
_W_FILE1 = PathTemplate('$rsrc/pdata/paper/sorted/$journal.pkl.gz')
assert _W_FILE0.substitute() == Journal._TAR_PATH
assert str(_W_FILE1) == str(Journal._ARTICLE_PATH)


def write(journal: Journal):
    if journal.path.is_file():
        print(journal.medline_ta, ': already done')
        return

    key = journal.medline_ta
    with gzip.open(journal.path, 'wb') as file:
        pickle.dump(journal.counts+1, file)
        pickle.dump(key, file)
        for article in ArticleFinder.from_journal(journal):
            pickle.dump(article, file)
    print(key)


def tar():
    with tarfile.open(Journal._TAR_PATH, 'w') as f:
        for j in Journal.unique_values():
            if j.path.is_file():
                f.add(j.path)


def main():
    with Pool(5) as p:
        p.map(write, Journal.unique_values())
    tar()
