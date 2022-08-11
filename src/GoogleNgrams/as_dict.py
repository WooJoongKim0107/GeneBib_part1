"""
postfix = ['_NOUN', '_VERB', '_ADJ', '_ADV', '_PRON', '_DET',
           '_ADP', '_NUM', '_CONJ', '_PRT', '_ROOT', '_START', '_END']

위와 같은 postfix로 끝나는 phrase들은 모두 postfix를 떼어낸 phrase도 기록되어 있다.
16 가지 예외가 존재했는데, 이들은 모두 Google 측에서 postfix를 붙이지 않았는데, 우연히 같은 문자열로 끝났던 경우들 같다.

BIN_TO_NUM
CONNECT_BY_ROOT
ID_INDICATOR_NUM
LOG_ARCHIVE_START
MYSQLI_NUM
OPTIMIZER_INDEX_COST_ADJ
P10_ACCESSION_NUM
SERVICE_DEMAND_START
SERVICE_BOOT_START
SERVICE_AUTO_START
SERVICE_SYSTEM_START
_NUM
XmALIGNMENT_END
_ROOT
_END
_START
"""
import gzip
import pickle
from mypathlib import PathTemplate
from multiprocessing import Pool

R_FILE = PathTemplate('$base/../google_ngrams/1grams/1-${i}-of-00024.gz', key='{:0>5}'.format)
# W_FILE = PathTemplate('$base/../google_ngrams/1grams/1-${i}-of-00024.pkl.gz', key='{:0>5}'.format)
W_FILE = PathTemplate('$base/../google_ngrams/1grams/1grams.pkl.gz', key='{:0>5}'.format).substitute()


def parse(line):
    k, *vs = line.rstrip().split('\t')
    # vs = [tuple(map(int, v.split(','))) for v in vs]
    years, matches, volumes = zip(*(v.split(',') for v in vs))
    # return k, {y: (m, v) for y, m, v in zip(years, matches, volumes)}
    return k, sum(map(int, matches))


def main(i):
    with gzip.open(R_FILE.substitute(i=i), 'rt') as file:
        w = {k: v for k, v in map(parse, file.readlines())}
    # with gzip.open(W_FILE.substitute(i=i), 'wb') as file:
    #     pickle.dump(w, file)
    # print(W_FILE.substitute(i=i))
    return w


if __name__ == '__main__':
    with Pool(24) as p:
        q = {}
        for dct in p.map(main, range(24)):
            q.update(dct)
    with gzip.open(W_FILE, 'wb') as file:
        pickle.dump(q, file)
