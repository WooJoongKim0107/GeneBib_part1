import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Epos import Epo
from Communities.containers import Community, Manager


R_FILE = {'epo_matches': PathTemplate('$pdata/epo/matched/epo_$index.pkl.gz'),
          'epo': PathTemplate('$pdata/epo/epo_$index.pkl.gz')}
_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE2 = PathTemplate('$data/curations/filtered/filtered.txt').substitute()
_R_FILE3 = PathTemplate('$lite/community/key2cmnt.pkl').substitute()
_R_FILE5 = PathTemplate('$lite/patent/cpc_tree.pkl').substitute()
_R_FILE6 = PathTemplate('$lite/patent/cpc_selected.pkl').substitute()
_RW_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()


def assign_epo_match(index, selected, manager):
    for pub_number, (title_matches, abstract_matches) in load_epo_matches(index, selected):
        manager.assign(pub_number, 'pub_numbers', *title_matches, *abstract_matches)
    return Community.CACHE


def load_epo_matches(index, selected):
    with gzip.open(R_FILE['epo_matches'].substitute(index=index), 'rb') as file:
        res = pickle.load(file)

    for pub_number, (title_matches, abstract_matches) in res.items():
        if pub_number in selected:
            yield pub_number, (title_matches, abstract_matches)


def get_e_args():
    manager = Manager()  # RW(R), Read2,3
    return [(i, Epo.load_selected(i), manager) for i in range(112)]  # Read5,6


def main():
    e_args = get_e_args()
    with Pool(10) as p:
        caches = p.starmap(assign_epo_match, e_args)
    Community.replace_cache(caches)
    Community.export_cache()


if __name__ == '__main__':
    main()
