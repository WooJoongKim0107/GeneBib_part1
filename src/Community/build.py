import pickle
from mypathlib import PathTemplate
from Community.containers import Community, Key2Cmnt, UnifyEqKeys


R_FILES = {'c2e': PathTemplate('$data/community/update_curated_cmnt_map_220914.pkl').substitute(),
           'c2k': PathTemplate('$data/community/cmnt_to_keyw_matchform_220915.pkl').substitute()}
_R_FILE0 = PathTemplate('$rsrc/pdata/uniprot/uniprot_sprot_parsed.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$rsrc/pdata/uniprot/uniprot_keywords.pkl').substitute()
_W_FILE0 = PathTemplate('$rsrc/lite/community/key2cmnt.pkl').substitute()


def build_cmnts():
    with open(R_FILES['c2e'], 'rb') as file:
        c2e: dict[int, tuple[str]] = pickle.load(file)
    with open(R_FILES['c2k'], 'rb') as file:
        c2k: dict[int, set[str]] = pickle.load(file)

    UnifyEqKeys.load()
    Community.load_entries()
    for cmnt_idx in (c2e.keys() | c2k.keys()):
        entries = c2e.get(cmnt_idx, ())
        unified_keywords = c2k.get(cmnt_idx, set())
        Community.from_parse(cmnt_idx, entries, unified_keywords)
    Community.export_cache()


def build_key2cmnt():
    k2c = Key2Cmnt(load=False)
    k2c.dump()


def main():
    build_cmnts()
    build_key2cmnt()


if __name__ == '__main__':
    main()
