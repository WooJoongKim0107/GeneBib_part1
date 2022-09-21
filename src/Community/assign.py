import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal
from Patents.temp_cpc import CPCTree
from Community.containers import Community, Manager


R_FILE = {'patent_matches': PathTemplate('$rsrc/pdata/patent/matched/patent_$index.pkl.gz'),
          'patent': PathTemplate('$rsrc/pdata/patent/patent_$index.pkl.gz')}
_R_FILE0 = PathTemplate('$rsrc/pdata/paper/matched/$journal.pkl.gz')


def assign_paper_match(js, manager):
    for j in js:
        for pmid, (title_matches, abstract_matches) in j.get_matches().items():
            manager.assign(pmid, 'pmid', *title_matches, *abstract_matches)
    return Community.CACHE


def assign_patent_match(index, selected, manager):
    for pub_number, (title_matches, abstract_matches) in load_patent_matches(index, selected):
        manager.assign(pub_number, 'pub_number', *title_matches, *abstract_matches)
    return Community.CACHE


def load_patent_matches(index, selected):
    with gzip.open(R_FILE['patent_matches'].substitute(index=index), 'rb') as file:
        res = pickle.load(file)
    for pub_number, (title_matches, abstract_matches) in res.items():
        if pub_number in selected:
            yield pub_number, (title_matches, abstract_matches)


def get_p_args():
    manager = Manager()
    return [(js, manager) for js in Journal.journals4mp(50, selected=True)]


def get_t_args():
    manager = Manager()
    cpc = CPCTree(load=True)
    for index in range(112):
        selected = get_selected_patents(index, cpc)
        yield index, selected, manager


def get_selected_patents(index, cpc: CPCTree):
    with gzip.open(R_FILE['patent'].substitute(index=index), 'rb') as file:
        chain = pickle.load(file)
    return {pub_number for pub_number, patent in chain.items() if cpc.any_selected(patent.cpcs)}


def main():
    p_args = get_p_args()
    with Pool(50) as p:
        caches = p.starmap(assign_paper_match, p_args)
    Community.merge_caches(*caches)

    t_args = get_t_args()
    with Pool(50) as p:
        caches = p.starmap(assign_patent_match, t_args)
    Community.merge_caches(*caches)
    Community.export_cache()


if __name__ == '__main__':
    main()
