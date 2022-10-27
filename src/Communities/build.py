import pickle
from mypathlib import PathTemplate
from Communities.containers import Community, UnifyEqKeys


R_FILES = {'c2e': PathTemplate('$data/community/update_curated_cmnt_map_220914.pkl').substitute(),
           'c2k': PathTemplate('$data/community/cmnt_to_keyw_matchform_221027.pkl').substitute()}
_R_FILE0 = PathTemplate('$pdata/uniprot/uniprot_keywords.pkl').substitute()
_W_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()


def build_cmnts():
    with open(R_FILES['c2e'], 'rb') as file:
        c2e: dict[int, tuple[str]] = pickle.load(file)
    with open(R_FILES['c2k'], 'rb') as file:
        c2k: dict[int, set[str]] = pickle.load(file)

    UnifyEqKeys.load()  # Read0
    for cmnt_idx in (c2e.keys() | c2k.keys()):
        entries = c2e.get(cmnt_idx, ())
        unified_keywords = c2k.get(cmnt_idx, set())
        Community.from_parse(cmnt_idx, entries, unified_keywords)
    Community.export_cache()  # Write0


def main():
    build_cmnts()


if __name__ == '__main__':
    main()
