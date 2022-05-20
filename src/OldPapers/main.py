import gzip, pickle
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers.main import get_article, parse
from . import START, STOP


R_FILE = PathTemplate('$rsrc/data/pubmed20n_gz/pubmed20n$number.xml.gz', key='{:0>4}'.format)
W_FILE = PathTemplate('$rsrc/pdata/pubmed20n_gz/article20n$number.pkl.gz', key='{:0>4}'.format)


def find_eng_articles(number):
    print(number)
    with gzip.open(R_FILE.substitute(number=number)) as file:
        tree = parse(file)
    return tree.getroot().findall("./PubmedArticle/MedlineCitation/Article/Language[.='eng']/../../..")


def write(number):
    res = [get_article(number, pubmed_article_elt) for pubmed_article_elt in find_eng_articles(number)]
    with gzip.open(W_FILE.substitute(number=number), 'wb') as file:
        pickle.dump(res, file)


def main():
    with Pool() as p:
        p.map(write, range(START, STOP))


if __name__ == '__main__':
    main()
