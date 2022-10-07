import pickle
from mypathlib import PathTemplate
from Communities.containers import Community, Key2Cmnt, UnifyEqKeys


R_FILES = {'c2e': PathTemplate('$rsrc/data/community/update_curated_cmnt_map_220914.pkl').substitute(),
           'c2k': PathTemplate('$rsrc/data/community/cmnt_to_keyw_matchform_221005.pkl').substitute()}
_R_FILE0 = PathTemplate('$rsrc/pdata/uniprot/uniprot_keywords.pkl').substitute()
_W_FILE0 = PathTemplate('$rsrc/pdata/community/community_cache.pkl.gz').substitute()
_W_FILE1 = PathTemplate('$rsrc/lite/community/key2cmnt.pkl').substitute()


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
    Key2Cmnt.build()  # Write1


if __name__ == '__main__':
    main()
