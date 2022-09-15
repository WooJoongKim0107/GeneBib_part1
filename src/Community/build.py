from Community.containers import Community, Key2Cmnt, ClusterEqKeys
from Community.parse import parse
from mypathlib import PathTemplate


R_FILE = PathTemplate('$data/community/clustering_results.txt').substitute()
_R_FILE0 = PathTemplate('$rsrc/pdata/uniprot/uniprot_sprot_parsed.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$rsrc/pdata/uniprot/uniprot_keywords.pkl').substitute()
_W_FILE0 = PathTemplate('$rsrc/lite/community/key2cmnt.pkl').substitute()


def build_cmnts():
    with open(R_FILE, 'r') as file:
        x = file.read()

    ClusterEqKeys.load()
    Community.load_entries()
    for cmnt_idx, entries, added, removed in parse(x):
        Community.from_parse(cmnt_idx, entries, added, removed)
    Community.export_cache()


def build_key2cmnt():
    k2c = Key2Cmnt(load=False)
    k2c.dump()


def main():
    build_cmnts()
    build_key2cmnt()


if __name__ == '__main__':
    main()
