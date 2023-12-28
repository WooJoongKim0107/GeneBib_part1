import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool
from Epos import Epo


_R_FILE = PathTemplate('$pdata/epo/epo_$index.pkl.gz')
W_FILE = PathTemplate('$lite/epo/granted.pkl').substitute()


def do(index):
    return {epo.pub_number: epo.is_granted for eponum, epo in Epo.load(index).items()}


def main():
    result = {}
    with Pool(10) as p:
        for res in p.imap_unordered(do, range(112)):
            result.update(res)

    with open(W_FILE, 'wb') as file:
        pickle.dump(result, file)


if __name__ == '__main__':
    main()
