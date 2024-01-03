import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from EpPatents import EpPatent
from Communities.containers import Community, Manager


R_FILE = {'ep_patent_matches': PathTemplate('$pdata/ep_patent/matched/patent_$index.pkl.gz'),
          'ep_patent': PathTemplate('$pdata/ep_patent/patent_$index.pkl.gz')}
_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE2 = PathTemplate('$data/curations/filtered/filtered.txt').substitute()
_R_FILE3 = PathTemplate('$lite/community/key2cmnt.pkl').substitute()
_R_FILE5 = PathTemplate('$lite/us_patent/cpc_tree.pkl').substitute()
_R_FILE6 = PathTemplate('$lite/us_patent/cpc_selected.pkl').substitute()
_RW_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()


def assign_ep_patent_match(index, selected, manager):
    for pub_number, (title_matches, abstract_matches) in load_ep_patent_matches(index, selected):
        manager.assign(pub_number, 'ep_pub_numbers', *title_matches, *abstract_matches)
    return Community.CACHE


def load_ep_patent_matches(index, selected):
    with gzip.open(R_FILE['ep_patent_matches'].substitute(index=index), 'rb') as file:
        res = pickle.load(file)

    for pub_number, (title_matches, abstract_matches) in res.items():
        if pub_number in selected:
            yield pub_number, (title_matches, abstract_matches)


def get_ep_args(use_cpc):
    manager = Manager()  # RW(R), Read2,3
    return [(i, EpPatent.load_selected(i, use_cpc=use_cpc), manager) for i in range(112)]  # Read5,6


def main(use_cpc):
    ep_args = get_ep_args(use_cpc)
    with Pool(10) as p:
        caches = p.starmap(assign_ep_patent_match, ep_args)
    Community.replace_cache(caches)
    Community.export_cache()


if __name__ == '__main__':
    main(use_cpc=False)
