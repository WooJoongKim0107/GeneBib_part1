import pickle
from itertools import chain
from mypathlib import PathTemplate
from Communities import Community

_R_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz')
W_FILES = {'paper': PathTemplate('$lite/paper/pmid2cmnt.pkl').substitute(),
           'us_patent': PathTemplate('$lite/us_patent/pubnum2cmnt.pkl').substitute(),
           'cn_patent': PathTemplate('$lite/cn_patent/pubnum2cmnt.pkl').substitute(),
           'ep_patent': PathTemplate('$lite/ep_patent/pubnum2cmnt.pkl').substitute(),}


def main():
    Community.import_cache()
    q, w, e, r = {}, {}, {}, {}
    for k, v in Community.items():
        for pmid in chain(*v.pmids.values()):
            q.setdefault(pmid, set()).add(k)
        for us_pub in chain(*v.us_pub_numbers.values()):
            w.setdefault(us_pub, set()).add(k)
        for cn_pub in chain(*v.cn_pub_numbers.values()):
            e.setdefault(cn_pub, set()).add(k)
        for ep_pub in chain(*v.ep_pub_numbers.values()):
            r.setdefault(ep_pub, set()).add(k)

    with open(W_FILES['paper'], 'wb') as file:
        pickle.dump(q, file)
    with open(W_FILES['us_patent'], 'wb') as file:
        pickle.dump(w, file)
    with open(W_FILES['cn_patent'], 'wb') as file:
        pickle.dump(e, file)
    with open(W_FILES['ep_patent'], 'wb') as file:
        pickle.dump(r, file)


if __name__ == '__main__':
    main()
