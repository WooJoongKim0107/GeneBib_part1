import gzip
import pickle
from textwrap import dedent, indent
from collections import Counter
from multiprocessing import Pool
from mypathlib import PathTemplate
from . import START, STOP


R_FILE = PathTemplate('$rsrc/pdata/paper/article22n$number.pkl.gz', key='{:0>4}'.format)
W_FILES = {'replica': PathTemplate('$rsrc/pdata/paper/paper_replicas.pkl'),
           'prints': PathTemplate('$rsrc/pdata/paper/paper_replicas.txt')}


class Replica(dict):
    """
    Replica: dict[pmid, list[Article]] for paper, dict[pub_number, list[Patents]] for patent
    """
    R_FILE = PathTemplate('')
    W_FILES = {'replica': PathTemplate(''), 'prints': PathTemplate('')}
    START = 0
    STOP = 0
    KEY_ATTR = ''

    def __init__(self, load=True):
        if load:
            super().__init__(self.load())
        else:
            collected_keys = self.collect_keys()
            self.locations = self.find_locations(collected_keys)
            super().__init__(self.get_replicas(self.locations))

    @classmethod
    def main(cls):
        q = cls(load=False)
        q.dump()
        with open(W_FILES['prints'].substitute(), 'w', encoding='UTF-8') as file:
            file.write(q.full_comparison())

    @classmethod
    def collect_keys(cls):
        with Pool() as p:
            return p.map(cls._collect_keys, range(cls.START, cls.STOP))

    @classmethod
    def find_locations(cls, collected_keys):
        counts = Counter(key for keys in collected_keys for key in keys)
        repeated_keys = {key for key, count in counts.items() if count > 1}
        return {i: itsc for i, keys in enumerate(collected_keys, start=cls.START)
                if (itsc := repeated_keys.intersection(keys))}

    @classmethod
    def get_replicas(cls, locations):
        with Pool() as p:
            lst_replicas = p.starmap(cls._get_replicas, locations.items())

        res = {}
        for replicas in lst_replicas:
            for key, x in replicas.items():
                res.setdefault(key, []).extend(x)
        return res

    def selected(self):
        return {k: v[-1] for k, v in self.items()}

    def compare(self):
        for k, containers in self.items():
            title = containers[0].title
            abstract = containers[0].abstract
            if any(_different(c.title, title) or _different(c.abstract, abstract) for c in containers):
                yield '\n'.join(c.info for c in containers)

    def full_comparison(self):
        x = [f'Case {i}:\n{indent(s, "    ")}' for i, s in enumerate(self.compare())]
        initial = dedent(f"""\
        {len(self):,} containers have one or more replicas.
        {len(x):,} have different title/abstract compared to their replicas.
        Below are the full list of .info of those {len(x):,} cases:""")
        return '\n\n'.join([initial]+x)

    def load(self):
        with open(self.W_FILES['replica'].substitute(), 'rb') as file:
            return pickle.load(file)

    def dump(self):
        with open(self.W_FILES['replica'].substitute(), 'wb') as file:
            pickle.dump(dict(self), file)

    @classmethod
    def read(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number)) as file:
            return pickle.load(file)

    @classmethod
    def _collect_keys(cls, number):
        return [getattr(c, cls.KEY_ATTR) for c in cls.read(number)]

    @classmethod
    def _get_replicas(cls, number, keys):
        containers = cls.read(number)

        replicas = {}
        for c in containers:
            if (key:=getattr(c, cls.KEY_ATTR)) in keys:
                replicas.setdefault(key, []).append(c)
        return replicas


def _different(s0, s1):
    _s0 = ''.join(s0.lower().split())
    _s1 = ''.join(s1.lower().split())
    return _s0 != _s1


class PaperReplica(Replica):
    R_FILE = R_FILE
    W_FILES = W_FILES
    START = START
    STOP = STOP
    KEY_ATTR = 'pmid'


main = PaperReplica.main

if __name__ == '__main__':
    main()
