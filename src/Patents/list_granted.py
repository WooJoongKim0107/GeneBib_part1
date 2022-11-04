import gzip
import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool


R_FILE = PathTemplate('$pdata/patent/patent_$index.pkl.gz')
W_FILE = PathTemplate('$lite/patent/granted.pkl').substitute()


def do(index):
    with gzip.open(R_FILE.substitute(index=index), 'rb') as file:
        chain = pickle.load(file)
    return {pat.pub_number: pat.is_granted for pubnum, pat in chain.items()}


def main():
    result = {}
    with Pool(10) as p:
        for res in p.imap_unordered(do, range(112)):
            result.update(res)

    with open(W_FILE, 'wb') as file:
        pickle.dump(result, file)


if __name__ == '__main__':
    main()
