import gzip
from lxml.etree import _Element as Element
from lxml.etree import parse
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP, Journal
from .parse import parse_journal
from .merge_journals import merge


R_FILE = PathTemplate('$data/paper/pubmed22n$number.xml.gz', key='{:0>4}'.format)
_W_FILE = PathTemplate('$pdata/paper/journal_cache.pkl.gz')


class Pioneer:
    R_FILE = PathTemplate('$data/paper/pubmed22n$number.xml.gz', key='{:0>4}'.format)
    _W_FILE = PathTemplate('$pdata/paper/journal_cache.pkl.gz')
    START = START
    STOP = STOP
    JNL = Journal

    @classmethod
    def read_and_explore(cls, number):
        root = cls.read(number)
        cls.explore(root)
        print(number)
        return cls.JNL.CACHE

    @classmethod
    def main(cls):
        with Pool(50) as p:
            caches = p.map(cls.read_and_explore, range(cls.START, cls.STOP))
        cls.JNL.replace_cache(caches)
        cls.JNL.CACHE = merge(cls.JNL.CACHE)
        cls.JNL.export_cache()  # Write

    @classmethod
    def read(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number)) as file:
            tree = parse(file)
        return tree.getroot()

    @classmethod
    def explore(cls, root: Element):
        for pubmed_article_elt in root:
            cls.JNL.from_parse(*parse_journal(pubmed_article_elt))


main = Pioneer.main


if __name__ == '__main__':
    main()
