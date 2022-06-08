import gzip
from lxml.etree import _Element as Element
from lxml.etree import parse
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP, Journal
from .parse import parse_journal
from .merge_journals import merge


R_FILE = PathTemplate('$rsrc/data/paper/pubmed22n$number.xml.gz', key='{:0>4}'.format)
_W_FILE = PathTemplate('$rsrc/pdata/paper/journal_cache.pkl.gz')
assert _W_FILE.substitute() == Journal._CACHE_PATH


class Pioneer:
    R_FILE = R_FILE
    _W_FILE = _W_FILE
    START = START
    STOP = STOP

    @classmethod
    def read_and_explore(cls, number):
        root = cls.read(number)
        cls.explore(root)
        print(number)
        return Journal._CACHE

    @classmethod
    def main(cls):
        with Pool(6) as p:
            caches = p.map(cls.read_and_explore, range(cls.START, cls.STOP))
        Journal._CACHE = Journal.merge_caches(*caches)
        Journal._CACHE = merge(Journal._CACHE)
        Journal.export_cache()

    @classmethod
    def read(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number)) as file:
            tree = parse(file)
        return tree.getroot()

    @classmethod
    def explore(cls, root: Element):
        for pubmed_article_elt in root:
            Journal.from_parse(*parse_journal(pubmed_article_elt))


main = Pioneer.main


if __name__ == '__main__':
    main()
