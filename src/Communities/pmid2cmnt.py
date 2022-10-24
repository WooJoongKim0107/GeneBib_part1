import pickle
from itertools import chain
from mypathlib import PathTemplate
from Communities import Community

_R_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz')
W_FILE0 = PathTemplate('$lite/paper/pmid2cmnt.pkl').substitute()
W_FILE1 = PathTemplate('$lite/patent/pubnum2cmnt.pkl').substitute()


def main():
    Community.import_cache()
    q, w = {}, {}
    for k, v in Community.items():
        for pmid in chain(*v.pmids.values()):
            q.setdefault(pmid, set()).add(k)
        for pub in chain(*v.pub_numbers.values()):
            w.setdefault(pub, set()).add(k)

    with open(W_FILE0, 'wb') as file:
        pickle.dump(q, file)
    with open(W_FILE1, 'wb') as file:
        pickle.dump(w, file)


if __name__ == '__main__':
    main()
