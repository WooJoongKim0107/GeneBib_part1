import pickle
from mypathlib import PathTemplate

with open(PathTemplate('$base/nested.pkl').substitute(), 'rb') as file:
    nested = pickle.load(file)

R_FILE = PathTemplate('$base/journals_selected.pkl')
W_FILE = PathTemplate('$rsrc/pdata/paper/matched/$journal.pkl.gz')


def gram_based_match2(target_text, target_code):
    target_match_list = bar(target_text, target_code)
    target_match_list.sort(key=_sort_key)
    filter_smaller(target_match_list)
    return target_match_list


def bar(target_text, target_code):
    target_match_list = []
    for x in nested.strict_matches2(target_text):  # x == (start, end), matched_text, match
        target_match_list.append((target_code, *x))
    return target_match_list


def _sort_key(match):
    start, end = match[1]
    return start, start-end  # No typo here


def filter_smaller(matches):
    initial, final, i = 0, -1, 0
    while i < len(matches):
        ini, fin = matches[i][1]
        if (initial <= ini) and (fin <= final):
            matches.pop(i)
        else:
            initial, final = ini, fin
            i += 1


if __name__ == '__main__':
    import gzip
    from Papers import Journal
    from multiprocessing import Pool

    # with open(R_FILE.substitute(), 'rb') as file:
    #     selected = pickle.load(file)
    with open(PathTemplate('$base/match_log.txt').substitute(), 'r') as file:
        already = file.read().splitlines()
    todo = [k for k in Journal.unique_keys() if k not in already]

    def match(medline_ta):
        res = {}
        journal = Journal[medline_ta]
        try:
            for art in journal.get_articles():
                title = gram_based_match2(art.title, 'title')
                abstract = gram_based_match2(art.abstract, 'abstract')
                if title or abstract:
                    res[art.pmid] = title, abstract
        except ValueError:
            raise ValueError(f'!!!ValueError: {medline_ta}-{art.pmid}')

        with gzip.open(W_FILE.substitute(journal=journal._simple_title), 'wb') as file:
            pickle.dump(res, file)
        print(medline_ta)

    with Pool(50) as p:
        p.map(match, todo)
