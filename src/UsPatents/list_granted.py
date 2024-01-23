import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool
from UsPatents import UsPatent


_R_FILE = PathTemplate('$pdata/us_patent/patent_$index.pkl.gz')
W_FILE = PathTemplate('$lite/us_patent/granted.pkl').substitute()


def do(index):
    return {us_pat.pub_number: us_pat.is_granted for pubnum, us_pat in UsPatent.load(index).items()}  # Read


def main():
    result = {}
    with Pool(10) as p:
        for res in p.imap_unordered(do, range(112)):
            result.update(res)

    with open(W_FILE, 'wb') as file:
        pickle.dump(result, file)


if __name__ == '__main__':
    main()
