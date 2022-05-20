import gzip
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers.pioneer import parse, explore
from Papers.merge_journals import merge
from . import START, STOP
from .containers import Journal


R_FILE = PathTemplate('$rsrc/data/pubmed20n_gz/pubmed20n$number.xml.gz', key='{:0>4}'.format)
_W_FILE = PathTemplate('$rsrc/pdata/pubmed20n_gz/journal_cache.pkl.gz')
assert _W_FILE.substitute() == Journal._CACHE_PATH


def read_and_explore(number):
    print(number)
    with gzip.open(R_FILE.substitute(number=number)) as file:
        tree = parse(file)
    explore(tree.getroot())
    return Journal._CACHE


def main():
    with Pool(6) as p:
        caches = p.map(read_and_explore, range(START, STOP))
    Journal._CACHE = Journal.merge_caches(*caches)
    Journal._CACHE = merge(Journal._CACHE)
    Journal.export_cache()


if __name__ == '__main__':
    main()
