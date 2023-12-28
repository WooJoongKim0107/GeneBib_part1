import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool
from EpPatents import EpPatent


_R_FILE = PathTemplate('$pdata/ep_patent/patent_$index.pkl.gz')
W_FILE = PathTemplate('$lite/ep_patent/granted.pkl').substitute()


def do(index):
    return {ep_pat.pub_number: ep_pat.is_granted for pubnum, ep_pat in EpPatent.load(index).items()}


def main():
    result = {}
    with Pool(10) as p:
        for res in p.imap_unordered(do, range(112)):
            result.update(res)

    with open(W_FILE, 'wb') as file:
        pickle.dump(result, file)


if __name__ == '__main__':
    main()
