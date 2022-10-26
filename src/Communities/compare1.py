import re
import sys
import pickle
from pathlib import Path
from itertools import groupby
from operator import itemgetter
from mypathlib import PathTemplate
from collections import Counter
from collections.abc import Generator
from multiprocessing import Pool


_R_FILE0 = PathTemplate('$data/hitgene_list/paper_hitgene_list.txt').substitute()
_R_FILE1 = PathTemplate('$data/hitgene_list/pap_cnt_dict_file_210112.pyc').substitute()
_R_FILE2 = sorted(Path('/home/data/01_data/OLD_HITS/paper/').glob('ppr*.pyc'))
_R_FILE_ = PathTemplate('/home/data/01_data/OLD_HITS/paper/ppr_${number}_${n}.pyc',
                        key=dict(number='{:0>4}'.format, n='{:0>2}'.format))
_W_FILE = PathTemplate('$lite/community/old_matched_papers/${cmnt_idx}.txt')


class GrepCmnt:
    R_FILES = {'0': PathTemplate('$data/hitgene_list/paper_hitgene_list.txt').substitute(),
               '1': PathTemplate('$data/hitgene_list/pap_cnt_dict_file_210112.pyc').substitute(),
               '2': sorted(Path('/home/data/01_data/OLD_HITS/paper/').glob('ppr*.pyc'))}
    W_FILE0 = PathTemplate('$lite/community/old_matched_papers/${cmnt_idx}.txt')
    W_FILE1 = PathTemplate('$lite/community/old_matched_papers/${cmnt_idx}.pkl')

    def __init__(self, cmnt_idx: int):
        self.cmnt_idx = cmnt_idx
        self.paper_ids = list(self.grep_cmnt())
        self.locations = self.get_locations()
        self.pypers = list(self.read())

    def inspect(self, target):
        c = Counter()
        for i, (loc, pyper) in enumerate(self.pypers):
            c += Counter(v.group() for t in pyper.get('TI', ['']) for v in re.finditer(target, t, re.I))
            c += Counter(v.group() for t in pyper.get('AB', ['']) for v in re.finditer(target, t, re.I))
        return c

    def grep_cmnt(self):
        """[paper_id...] hit"""
        with open(self.R_FILES['0'], 'r') as file:
            lines = (map(int, line.split(',')) for line in file.read().splitlines()[1:])
            yield from (paper_id for paper_id, year, *cmnts in lines if self.cmnt_idx in set(cmnts))

    def get_locations(self):
        """[(number, i)...] for each paper_id"""
        with open(self.R_FILES['1'], 'rb') as file:
            finder = {v: k for k, v in pickle.load(file).items()}
        return sorted(finder[paper_id] for paper_id in self.paper_ids)

    def read(self) -> Generator[dict]:
        for number, eyes in groupby(self.locations, key=itemgetter(0)):
            with open(self.R_FILES['2'][number], 'rb') as file:
                pypers = pickle.load(file)
            for _, i in eyes:
                yield (number, i), pypers[i]

    def write(self):
        """file.write(title)"""
        with open(self.W_FILE0.substitute(cmnt_idx=self.cmnt_idx), 'wt') as file:
            for loc, pyper in self.pypers:
                for x in self._get(loc, pyper):
                    file.write(x+'\n')
        return self

    def dump(self):
        with open(self.W_FILE1.substitute(cmnt_idx=self.cmnt_idx), 'wb') as file:
            pickle.dump(self, file)
        return self

    @classmethod
    def load(cls, cmnt_idx):
        with open(cls.W_FILE1.substitute(cmnt_idx=cmnt_idx), 'rb') as file:
            return pickle.load(file)

    @staticmethod
    def _get(loc, pyper):
        if 'TI' in pyper:
            return pyper['TI']
        elif 'AB' in pyper:
            return [f'TitleNotFound: {x}' for x in pyper['AB']]
        else:
            return [f'NeitherFoundError: {loc}']


def main(cmnt_idx):
    GrepCmnt(cmnt_idx).dump().write()


def p_main():
    cmnt_indices = {int(x) for x in sys.argv[1:]}
    n = min(50, len(cmnt_indices))
    with Pool(n) as p:
        p.map(main, cmnt_indices)


if __name__ == '__main__':
    p_main()
