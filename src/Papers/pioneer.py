import gzip
from lxml.etree import _Element as Element
from lxml.etree import parse
from multiprocessing import Pool
from mypathlib import PathTemplate
from .containers import Journal
from .parse import parse_journal
from .merge_journals import merge


R_FILE = PathTemplate('$rsrc/data/paper/pubmed22n$number.xml.gz', key='{:0>4}'.format)
_W_FILE = PathTemplate('$rsrc/pdata/paper/journal_cache.pkl.gz')


def explore(root: Element):
    for pubmed_article_elt in root:
        Journal.from_parse(*parse_journal(pubmed_article_elt))


def read_and_explore(number):
    print(number)
    with gzip.open(R_FILE.substitute(number=number)) as file:
        tree = parse(file)
    explore(tree.getroot())
    return Journal._CACHE


def main():
    with Pool(6) as p:
        caches = p.map(read_and_explore, range(1, 1115))
    Journal._CACHE = Journal.merge_caches(*caches)
    Journal._CACHE = merge(Journal._CACHE)
    Journal.export_cache()


if __name__ == '__main__':
    main()
