import pickle
from itertools import chain
from mypathlib import PathTemplate
from Communities import Community

_R_FILE = PathTemplate('$rsrc/pdata/community/community_cache.pkl.gz')
W_FILE = PathTemplate('$rsrc/lite/paper/pmid2cmnt.pkl').substitute()


def main():
    Community.import_cache()
    q = {}
    for k, v in Community.items():
        for pmid in chain(*c.pmids.values()):
            q.setdefault(pmid, set()).add(k)

    with open(W_FILE, 'wb') as file:
        pickle.dump(q, file)


if __name__ == '__main__':
    main()
