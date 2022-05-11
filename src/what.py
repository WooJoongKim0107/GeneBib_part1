import gzip
import pickle
from multiprocessing import Pool
from collections import Counter
from lxml.etree import parse, tostring
from Papers.main import R_FILE
from mypathlib import PathTemplate


W_FILE = PathTemplate('$rsrc/pdata/paper/statistics.pkl')


def get_tree(number):
    print(number)
    with gzip.open(R_FILE.substitute(number=number)) as file:
        tree = parse(file)
    return tree


def print_element(x):
    print(tostring(x, pretty_print=True).decode())


def counts(number):
    tree = get_tree(number)
    engs = tree.getroot().findall("./PubmedArticle/MedlineCitation/Article/Language[.='eng']/../../..")
    pmids = Counter(x.findtext('./MedlineCitation/PMID') for x in engs)

    x = len(tree.getroot())
    y = len(engs)
    z = len(pmids)
    return number, x, y, z


def main():
    with Pool() as p:
        res = p.map(counts, range(1, 1115))
    with gzip.open(W_FILE.substitute(), 'wb') as file:
        pickle.dump(res, file)


if __name__ == '__main__':
    main()
