import gzip
import pickle
from multiprocessing import Pool
from lxml.etree import _Element as Element
from lxml.etree import parse
from mypathlib import PathTemplate
from . import START, STOP
from .containers import Article, Journal
from .parse import parse_article, find_journal_key


R_FILE = PathTemplate('$rsrc/data/paper/pubmed22n$number.xml.gz', key='{:0>4}'.format)
W_FILE = PathTemplate('$rsrc/pdata/paper/article22n$number.pkl.gz', key='{:0>4}'.format)
MSG = PathTemplate('$rsrc/pdata/paper/article22n$number.txt', key='{:0>4}'.format)


class Refine:
    R_FILE = R_FILE
    W_FILE = W_FILE
    MSG = MSG
    START = START
    STOP = STOP

    @classmethod
    def read_eng_articles(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number)) as file:
            tree = parse(file)
        return tree.getroot().findall("./PubmedArticle/MedlineCitation/Article/Language[.='eng']/../../..")

    @classmethod
    def refine_article(cls, number, pubmed_article_elt: Element):
        article = Article.from_parse(*parse_article(pubmed_article_elt))
        article.location = number
        article._journal_title = cls.report_journal(number, pubmed_article_elt, article.pmid)
        return article

    @classmethod
    def write(cls, number):
        eng_arts = cls.read_eng_articles(number)
        res = [cls.refine_article(number, pubmed_article_elt) for pubmed_article_elt in eng_arts]
        with gzip.open(cls.W_FILE.substitute(number=number), 'wb') as file:
            pickle.dump(res, file)
        print(number)

    @classmethod
    def main(cls):
        with Pool(6) as p:
            p.map(cls.write, range(cls.START, cls.STOP))

    @classmethod
    def report_journal(cls, number, pubmed_article_elt: Element, pmid):
        key = find_journal_key(pubmed_article_elt)
        if key not in Journal:
            with open(cls.MSG.substitute(number), 'a') as file:
                file.write(f'Cannot find appropriate Journal for {number}: {pmid}\n')
        return key


main = Refine.main


if __name__ == '__main__':
    main()