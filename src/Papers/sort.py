import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal  # Read0
from Papers.article_finder import ArticleFinder


_W_FILE = PathTemplate('$rsrc/pdata/paper/sorted/$journal.pkl.gz')


def write(journal: Journal):
    if journal.art_path.is_file():
        print(journal.medline_ta, ': already done')
        return

    with gzip.open(journal.art_path, 'wb') as file:  # Write
        pickle.dump(journal.counts+1, file)
        pickle.dump(journal.medline_ta, file)
        for article in ArticleFinder.from_journal(journal.medline_ta):
            pickle.dump(article, file)
    print(journal._simple_title)


def main():
    with Pool(5) as p:
        p.starmap(write, Journal.unique_values())


if __name__ == '__main__':
    main()
