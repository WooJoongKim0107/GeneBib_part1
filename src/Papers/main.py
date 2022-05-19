import gzip
import pickle
from multiprocessing import Pool
from lxml.etree import _Element as Element
from lxml.etree import parse
from mypathlib import PathTemplate
from .containers import Article, Journal
from .parse import parse_article


R_FILE = PathTemplate('$rsrc/data/paper/pubmed22n$number.xml.gz', key='{:0>4}'.format)
W_FILE = PathTemplate('$rsrc/pdata/paper/article22n$number.pkl.gz', key='{:0>4}'.format)


def get_article(number, pubmed_article_elt: Element):
    article = Article.from_parse(*parse_article(pubmed_article_elt))
    article.location = number

    journal = find_journal(number, pubmed_article_elt, article.pmid)
    article.journal = journal
    return article


def find_eng_articles(number):
    print(number)
    with gzip.open(R_FILE.substitute(number=number)) as file:
        tree = parse(file)
    return tree.getroot().findall("./PubmedArticle/MedlineCitation/Article/Language[.='eng']/../../..")


def find_journal(number, pubmed_article_elt: Element, pmid):
    medline_ta: str = pubmed_article_elt.findtext('./MedlineCitation/MedlineJournalInfo/MedlineTA')
    if medline_ta in Journal._CACHE:
        return Journal(medline_ta)

    iso_abbreviation: str | None = pubmed_article_elt.findtext('./MedlineCitation/Article/Journal/ISOAbbreviation')
    if iso_abbreviation in Journal._CACHE:
        return Journal(iso_abbreviation)

    title: str | None = pubmed_article_elt.findtext('./MedlineCitation/Article/Journal/Title')
    if title in Journal._CACHE:
        return Journal(title)
    else:
        raise KeyError(f'Cannot find appropriate Journal for {number}: {pmid}')


def write(number):
    res = [get_article(number, pubmed_article_elt) for pubmed_article_elt in find_eng_articles(number)]
    with gzip.open(W_FILE.substitute(number=number), 'wb') as file:
        pickle.dump(res, file)


def main():
    with Pool() as p:
        p.map(write, range(1, 1115))


if __name__ == '__main__':
    main()