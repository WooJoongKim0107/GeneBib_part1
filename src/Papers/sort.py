import gzip
import pickle
from multiprocessing import Pool
from myclass import TarRW
from mypathlib import PathTemplate
from Papers import Journal  # Read0
from Papers.article_finder import ArticleFinder


_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$pdata/paper/paper_$index.pkl.gz')
_R_FILE2 = PathTemplate('$lite/paper/journal_to_article.pkl')
_R_FILE3 = PathTemplate('$lite/paper/article_to_index.pkl')
_W_FILE = PathTemplate('$pdata/paper/sorted/$journal.pkl.gz')
W_FILE0 = PathTemplate('$pdata/paper/sorted/journal.tar').substitute()


def already_written(journal: Journal):
    if journal.art_path.is_file():
        with gzip.open(journal.art_path, 'rb') as file:
            counts = pickle.load(file) - 1
            medline_ta = pickle.load(file)
            if counts == journal.counts and medline_ta == journal.medline_ta:
                try:
                    length = 0
                    while length <= counts:  # length == num of success
                        pickle.load(file)
                        length += 1

                except EOFError:
                    return length == counts
    return False


def write(journal: Journal):
    if already_written(journal):
        return journal, 0

    with gzip.open(journal.art_path, 'wb') as file:  # Write
        pickle.dump(journal.counts+1, file)
        pickle.dump(journal.medline_ta, file)
        c = 0
        for article in ArticleFinder.from_journal(journal.medline_ta):
            pickle.dump(article, file)
            c += 1
        if c != journal.counts:
            raise ValueError(f'{journal.medline_ta}: Invalid!  {journal.art_path}')
    return journal, 1


def main():
    paths = []
    with Pool(50) as p:
        for j, code in p.imap_unordered(write, Journal.unique_values()):
            if code:
                print(j.medline_ta)
            else:
                print(j.medline_ta, ': already done')
            paths.append(j.art_path)

    with TarRW(W_FILE0, 'w') as q:
        q.extend(paths)


if __name__ == '__main__':
    main()
