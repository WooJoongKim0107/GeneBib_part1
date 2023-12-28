import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal  # Read0
from UsPatents import UsPatent
from Communities.containers import Community, Manager


R_FILE = {'patent_matches': PathTemplate('$pdata/us_patent/matched/patent_$index.pkl.gz'),
          'patent': PathTemplate('$pdata/us_patent/patent_$index.pkl.gz')}
_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$pdata/paper/matched/$journal.pkl.gz')
_R_FILE2 = PathTemplate('$data/curations/filtered/filtered.txt').substitute()
_R_FILE3 = PathTemplate('$lite/community/key2cmnt.pkl').substitute()
_R_FILE4 = PathTemplate('$lite/paper/jnls_selected.pkl').substitute()
_R_FILE5 = PathTemplate('$lite/us_patent/cpc_tree.pkl').substitute()
_R_FILE6 = PathTemplate('$lite/us_patent/cpc_selected.pkl').substitute()
_RW_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()


def assign_paper_match(js, manager):
    for j in js:
        for pmid, (title_matches, abstract_matches) in j.get_matches().items():  # Read1
            manager.assign(pmid, 'pmids', *title_matches, *abstract_matches)
    return Community.CACHE


def assign_patent_match(index, selected, manager):
    for pub_number, (title_matches, abstract_matches) in load_patent_matches(index, selected):
        manager.assign(pub_number, 'pub_numbers', *title_matches, *abstract_matches)
    return Community.CACHE


def load_patent_matches(index, selected):
    with gzip.open(R_FILE['patent_matches'].substitute(index=index), 'rb') as file:
        res = pickle.load(file)

    for pub_number, (title_matches, abstract_matches) in res.items():
        if pub_number in selected:
            yield pub_number, (title_matches, abstract_matches)


def get_p_args():
    manager = Manager()  # RW(R), Read2,3
    return [(js, manager) for js in Journal.journals4mp(50, selected=True)]  # Read4


def get_t_args():
    manager = Manager()  # RW(R), Read2,3
    return [(i, UsPatent.load_selected(i), manager) for i in range(112)]  # Read5,6


def main():
    p_args = get_p_args()
    with Pool(50) as p:
        caches = p.starmap(assign_paper_match, p_args)
    Community.replace_cache(caches)

    t_args = get_t_args()
    with Pool(10) as p:
        caches = p.starmap(assign_patent_match, t_args)
    Community.replace_cache(caches)
    Community.export_cache()


if __name__ == '__main__':
    main()
