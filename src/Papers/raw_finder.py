import gzip
from multiprocessing import Pool
from lxml.etree import tostring, parse
from . import START, STOP
from .main import R_FILE


class RawFinder:
    START = START
    STOP = STOP
    R_FILE = R_FILE

    def __init__(self, number):
        self.root = self.get(number)

    @classmethod
    def get(cls, number):
        with gzip.open(cls.R_FILE.substitute(number=number)) as file:
            return parse(file).getroot()

    def findall(self, attr, val):
        return self.root.findall(f'.//{attr}[.="{val}"]')

    def findall_whose(self, attr, val):
        parents = [x.getparent() for x in self.findall(attr, val)]
        while parents[0].tag != 'PubmedArticle':
            parents = [x.getparent() for x in parents]
        return parents

    def tostring_whose(self, attr, val):
        lst = self.findall_whose(attr, val)
        return '\n'.join([tostring(x, encoding='UTF-8', pretty_print=True).decode() for x in lst])

    def savetxt_whose(self, attr, val, path):
        with open(path, 'w', encoding='UTF-8') as file:
            file.write(self.tostring_whose(attr, val))

    @classmethod
    def _init_tostring_whose(cls, number, attr, val):
        return cls(number).tostring_whose(attr, val)

    @classmethod
    def mp_tostring_whose(cls, attr, val, processes=5):
        args = [(number, attr, val) for number in range(cls.START, cls.STOP)]
        with Pool(processes) as p:
            return '\n'.join(p.starmap(cls._init_tostring_whose, args))

    @classmethod
    def mp_savetxt_whose(cls, attr, val, path, processes=5):
        if not path.endswith('.gz'):
            raise ValueError('mp_savetxt_whose() must be saved in .gz format')
        s = cls.mp_tostring_whose(attr, val, processes=processes)
        with gzip.open(path, 'wt', encoding='UTF-8') as file:
            file.write(s)
