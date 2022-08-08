import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal
from UniProt.containers import Nested, Match


nested = Nested(True)

R_FILE = PathTemplate('$base/journals_selected.pkl')
W_FILE = PathTemplate('$rsrc/pdata/paper/matched/$journal.pkl.gz')


def match_and_filter(target_text):
    target_match_list = list(nested.strict_matches(target_text))
    target_match_list.sort(key=_sort_key)
    filter_smaller(target_match_list)
    return target_match_list


def _sort_key(match: Match):
    start, end = match.spans
    return start, start-end  # No typo here


def filter_smaller(matches: list[Match]):
    initial, final, i = 0, -1, 0
    while i < len(matches):
        ini, fin = matches[i].spans
        if (initial <= ini) and (fin <= final):
            matches.pop(i)
        else:
            initial, final = ini, fin
            i += 1


def main(medline_ta):
    res = {}
    journal = Journal[medline_ta]
    for art in journal.get_articles():
        title = match_and_filter(art.title)
        abstract = match_and_filter(art.abstract)
        if title or abstract:
            res[art.pmid] = title, abstract

    with gzip.open(W_FILE.substitute(journal=journal._simple_title), 'wb') as file:
        pickle.dump(res, file)
    print(medline_ta)


if __name__ == '__main__':
    already = set()
    # with open(PathTemplate('$base/match_log.txt').substitute(), 'r') as file:
    #     already = file.read().splitlines()
    todo = [k for k in Journal.unique_keys() if k not in already]

    with Pool(50) as p:
        p.map(main, todo)
