import pickle
from itertools import chain
from mypathlib import PathTemplate
from Communities import Community

_R_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz')
W_FILES = {'paper': PathTemplate('$lite/paper/pmid2cmnt.pkl').substitute(),
           'patent': PathTemplate('$lite/patent/pubnum2cmnt.pkl').substitute(),
           'epo': PathTemplate('$lite/epo/pubnum2cmnt.pkl').substitute()}


def main():
    Community.import_cache()
    q, w, e = {}, {}, {}
    for k, v in Community.items():
        for pmid in chain(*v.pmids.values()):
            q.setdefault(pmid, set()).add(k)
        for pub in chain(*v.pub_numbers.values()):
            w.setdefault(pub, set()).add(k)
        for epo in chain(*v.epo_pub_numbers.values()):
            e.setdefault(epo, set()).add(k)

    with open(W_FILES['paper'], 'wb') as file:
        pickle.dump(q, file)
    with open(W_FILES['patent'], 'wb') as file:
        pickle.dump(w, file)
    with open(W_FILES['epo'], 'wb') as file:
        pickle.dump(e, file)


if __name__ == '__main__':
    main()
