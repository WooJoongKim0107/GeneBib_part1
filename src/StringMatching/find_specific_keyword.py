"""
python -m StringMatching.find_specific_keyword Mater > ../Mater.logs

<../Mater.logs>
    Import Journal cache from: /home/wjkim/GeneBib_sync/rsrc/pdata/paper/journal_cache.pkl.gz
    target = Mater
    https://pubmed.ncbi.nlm.nih.gov/33326714/        (N Engl J Med)
    https://pubmed.ncbi.nlm.nih.gov/12133995/        (J Immunol)
    https://pubmed.ncbi.nlm.nih.gov/21496916/        (Lancet)
    https://pubmed.ncbi.nlm.nih.gov/17303084/        (Biochem Biophys Res Commun)
    https://pubmed.ncbi.nlm.nih.gov/26354724/        (Sci Rep)
    https://pubmed.ncbi.nlm.nih.gov/19164568/        (Proc Natl Acad Sci U S A)
    https://pubmed.ncbi.nlm.nih.gov/22492928/        (J Virol)
    https://pubmed.ncbi.nlm.nih.gov/15477400/        (Circulation)
"""
from itertools import chain
from operator import attrgetter
from more_itertools import unique_everseen
from multiprocessing import Pool
from mypathlib import PathTemplate
from Papers import Journal


class MatchedKeywordFinder:
    PATH = PathTemplate('$base/match_key_finder/${target}.logs')

    def __init__(self, *targets):
        self.targets = set(targets)
        self._getter = attrgetter('text')

    def find_target(self, js):
        for j in js:
            done = set()
            for pmid, matches in j.get_matches().items():
                for match in unique_everseen(chain(*matches), key=self._getter):
                    if (match.text not in done) and (match.text in self.targets):
                        done.add(match.text)
                        url = f'https://pubmed.ncbi.nlm.nih.gov/{pmid}/'
                        print(f'{url:<45}    ({j.medline_ta})',
                              file=self.PATH.substitute(target=match.text).open('a'))

    def mp_find_target(self, n):
        args = Journal.journals4mp(n, selected=True)
        with Pool(n) as p:
            p.map(self.find_target, args)


def main():
    from GoogleNgrams.filtering import list_up
    summary, _ = list_up()
    mkf = MatchedKeywordFinder(*(v[0] for v in summary[1]))
    mkf.mp_find_target(60)


if __name__ == '__main__':
    import sys
    targets = sys.argv[1:]
    q = MatchedKeywordFinder(*targets)
    q.mp_find_target(50)
