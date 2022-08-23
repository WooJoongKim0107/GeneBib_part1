"""
postfix = ['_NOUN', '_VERB', '_ADJ', '_ADV', '_PRON', '_DET',
           '_ADP', '_NUM', '_CONJ', '_PRT', '_ROOT', '_START', '_END']

위와 같은 postfix로 끝나는 phrase들은 모두 postfix를 떼어낸 phrase도 기록되어 있다.
16 가지 예외가 존재했는데, 이들은 모두 Google 측에서 postfix를 붙이지 않았는데, 우연히 같은 문자열로 끝났던 경우들 같다.
어차피 우리의 용도로는 필요하지 않을 것이므로, postfix로 끝나는 phrase는 parse 하지 않도록 한다.

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

R_FILES = {'1gram': PathTemplate('/home/data/01_data/google_ngrams/1grams/1-${i}-of-00024.gz', key='{:0>5}'.format),
           '2gram': PathTemplate('/home/data/01_data/google_ngrams/2grams/2-${i}-of-00589.gz', key='{:0>5}'.format)}

W_FILES = {'total': PathTemplate('$base/google_ngram_counts.pkl.gz').substitute(),
           '1gram': PathTemplate('/home/data/01_data/google_ngrams/1grams/1grams.pkl.gz', key='{:0>5}'.format).substitute(),
           '2gram': PathTemplate('/home/data/01_data/google_ngrams/2grams/2grams.pkl.gz', key='{:0>5}'.format).substitute()}


POSTFIXES = ['_NOUN', '_VERB', '_ADJ', '_ADV', '_PRON', '_DET',
             '_ADP', '_NUM', '_CONJ', '_PRT', '_ROOT', '_START', '_END']


def parse(line):
    k, *vs = line.rstrip().split('\t')
    # vs = [tuple(map(int, v.split(','))) for v in vs]
    years, matches, volumes = zip(*(v.split(',') for v in vs))
    # return k, {y: (m, v) for y, m, v in zip(years, matches, volumes)}
    return k, sum(map(int, matches))


def main(n, i):
    with gzip.open(R_FILES[f'{n}gram'].substitute(i=i), 'rt') as file:
        w = {k: v for k, v in map(parse, file.readlines()) if not any(k.endswith(post) for post in POSTFIXES)}
    # with gzip.open(W_FILE.substitute(i=i), 'wb') as file:
    #     pickle.dump(w, file)
    # print(W_FILE.substitute(i=i))
    return w


if __name__ == '__main__':
    total = {}
    for n in (1, 2):
        max_i = 24 if n == 1 else 589 if n == 2 else None
        args = [(n, i) for i in range(max_i)]
        with Pool(50) as p:
            q = {}
            for dct in p.starmap(main, args):
                q.update(dct)
        total.update(q)
        with gzip.open(W_FILES[f'{n}gram'], 'wb') as file:
            pickle.dump(q, file)
    with gzip.open(W_FILES['total'], 'wb') as file:
        pickle.dump(total, file)
