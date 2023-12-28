import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool
from CnPatents import CnPatent


_R_FILE = PathTemplate('$pdata/cn_patent/patent_$index.pkl.gz')
W_FILE = PathTemplate('$lite/cn_patent/granted.pkl').substitute()


def do(index):
    return {cn_pat.pub_number: cn_pat.is_granted for pubnum, cn_pat in CnPatent.load(index).items()}


def main():
    result = {}
    with Pool(10) as p:
        for res in p.imap_unordered(do, range(112)):
            result.update(res)

    with open(W_FILE, 'wb') as file:
        pickle.dump(result, file)


if __name__ == '__main__':
    main()
