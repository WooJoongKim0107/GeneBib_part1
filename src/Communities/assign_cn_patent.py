import gzip
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from CnPatents import CnPatent
from Communities.containers import Community, Manager


R_FILE = {'cn_patent_matches': PathTemplate('$pdata/cn_patent/matched/patent_$index.pkl.gz'),
          'cn_patent': PathTemplate('$pdata/cn_patent/patent_$index.pkl.gz')}
_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE2 = PathTemplate('$data/curations/filtered/filtered.txt').substitute()
_R_FILE3 = PathTemplate('$lite/community/key2cmnt.pkl').substitute()
_R_FILE5 = PathTemplate('$lite/us_patent/cpc_tree.pkl').substitute()
_R_FILE6 = PathTemplate('$lite/us_patent/cpc_selected.pkl').substitute()
_RW_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()


def assign_cn_patent_match(index, selected, manager):
    for pub_number, (title_matches, abstract_matches) in load_cn_patent_matches(index, selected):
        manager.assign(pub_number, 'cn_pub_numbers', *title_matches, *abstract_matches)
    return Community.CACHE


def load_cn_patent_matches(index, selected):
    with gzip.open(R_FILE['cn_patent_matches'].substitute(index=index), 'rb') as file:
        res = pickle.load(file)

    for pub_number, (title_matches, abstract_matches) in res.items():
        if pub_number in selected:
            yield pub_number, (title_matches, abstract_matches)


def get_cn_args(use_cpc):
    manager = Manager()  # RW(R), Read2,3
    return [(i, CnPatent.load_selected(i, use_cpc=use_cpc), manager) for i in range(112)]  # Read5,6


def main(use_cpc):
    cn_args = get_cn_args(use_cpc)
    with Pool(10) as p:
        caches = p.starmap(assign_cn_patent_match, cn_args)
    Community.replace_cache(caches)
    Community.export_cache()


if __name__ == '__main__':
    main(use_cpc=False)
