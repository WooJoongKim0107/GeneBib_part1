import gzip
import json
import pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Patents.containers import Patent
from Patents.parse import parse_patent

R_FILE = PathTemplate('$rsrc/data/patent/patent_202111_$number.json.gz', key='{:0>12}'.format)
W_FILE = PathTemplate('$rsrc/pdata/patent/patent_202111_$number.pkl.gz', key='{:0>12}'.format)


def get_patent(x):
    patent = Patent.from_parse(*parse_patent(x))
    return patent.pub_number, patent


def read(number):
    print(number)
    with gzip.open(R_FILE.substitute(number=number), 'rt', encoding='UTF-8') as file:
        q = [json.loads(line) for line in file]
    return q


def write(number):
    patents = dict(get_patent(x) for x in read(number))
    with gzip.open(W_FILE.substitute(number=number), 'wb') as file:
        pickle.dump(patents, file)


def main():
    with Pool(6) as p:
        p.map(write, range(10))  #10033


if __name__ == '__main__':
    main()
