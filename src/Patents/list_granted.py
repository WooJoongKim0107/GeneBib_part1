import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool
from Patents import Patent


_R_FILE = PathTemplate('$pdata/patent/patent_$index.pkl.gz')
W_FILE = PathTemplate('$lite/patent/granted.pkl').substitute()


def do(index):
    return {pat.pub_number: pat.is_granted for pubnum, pat in Patent.load(index).items()}


def main():
    result = {}
    with Pool(10) as p:
        for res in p.imap_unordered(do, range(112)):
            result.update(res)

    with open(W_FILE, 'wb') as file:
        pickle.dump(result, file)


if __name__ == '__main__':
    main()
