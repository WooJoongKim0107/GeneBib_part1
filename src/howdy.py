import gzip
import pickle
import random
from more_itertools import nth
from mypathlib import PathTemplate

r = PathTemplate('$rsrc/pdata/$mtype/${mtype}_$number.pkl.gz')
w = PathTemplate('$base/medline_dates.txt')


def read(mtype, number):
    with gzip.open(r.substitute(mtype=mtype, number=number)) as file:
        return pickle.load(file)


def random_access(chain):
    i = random.randint(0, len(chain.maps) - 1)
    n = random.randint(0, len(chain.maps[i]) - 1)
    return nth(chain.maps[i].values(), n)


def random_info(chain):
    return random_access(chain).info()


def check_len(chain):
    return len(chain) == sum(len(x) for x in chain.maps)


def check_empty(chain):
    title = sum(1 for x in chain.values() if not x.article_title)
    abstract = sum(1 for x in chain.values() if not x.abstract)
    return title, abstract, len(chain)
