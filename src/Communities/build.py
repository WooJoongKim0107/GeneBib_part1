import pickle
from mypathlib import PathTemplate
from Communities.containers import Community, UnifyEqKeys, C2E, C2K


_R_FILES = {'c2e': PathTemplate('$data/community/update_curated_cmnt_map_220914.pkl').substitute(),
            'c2k': PathTemplate('$data/community/cmnt_to_keyw_matchform_221122.pkl').substitute()}
_R_FILE0 = PathTemplate('$pdata/uniprot/uniprot_keywords.pkl').substitute()
_W_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()


def build_cmnts():
    c2e: dict[int, tuple[str]] = C2E.load()
    c2k: dict[int, set[str]] = C2K.load()

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
