import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal  # Read0
from Papers.article_finder import ArticleFinder


_R_FILE0 = PathTemplate('$rsrc/pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$rsrc/pdata/paper/paper_$index.pkl.gz')
_R_FILE2 = PathTemplate('$rsrc/lite/paper/journal_to_article.pkl')
_R_FILE3 = PathTemplate('$rsrc/lite/paper/article_to_index.pkl')
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
    with Pool(50) as p:
        p.map(write, Journal.unique_values())


if __name__ == '__main__':
    main()
