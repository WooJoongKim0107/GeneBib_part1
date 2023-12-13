from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal  # Read0
from Communities.containers import Community, Manager


_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$pdata/paper/matched/$journal.pkl.gz')
_R_FILE2 = PathTemplate('$data/curations/filtered/filtered.txt').substitute()
_R_FILE3 = PathTemplate('$lite/community/key2cmnt.pkl').substitute()
_R_FILE4 = PathTemplate('$lite/paper/jnls_selected.pkl').substitute()
_RW_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()


def assign_paper_match(js, manager):
    for j in js:
        for pmid, (title_matches, abstract_matches) in j.get_matches().items():  # Read1
            manager.assign(pmid, 'pmids', *title_matches, *abstract_matches)
    return Community.CACHE


def get_p_args():
    manager = Manager()  # RW(R), Read2,3
    return [(js, manager) for js in Journal.journals4mp(50, selected=True)]  # Read4


def main():
    p_args = get_p_args()
    with Pool(50) as p:
        caches = p.starmap(assign_paper_match, p_args)
    Community.replace_cache(caches)
    Community.export_cache()


if __name__ == '__main__':
    main()
