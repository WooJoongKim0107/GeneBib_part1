"""
                     them4	['A1A4L1', 'Q5T1C6', 'Q3UUI3', 'Q566R0', 'Q6GLK2']
                     them6	['A6H707', 'Q8WUY1', 'B0JZP3', 'Q6NRK8', 'B0BLZ5', 'Q80ZW2', 'Q5XIE1']
                     manea	['Q5SRI9', 'Q5RD93', 'Q6NXH2', 'Q5GF25', 'Q6DE40']
                     sub10	['D4AQG0', 'C5FX37', 'D4DIS6']
                     slip1	['A9UHW6', 'Q8MR31']
            gamma-2-globin	['P68258', 'P62742', 'P69892', 'P61948', 'P61921', 'P18996', 'P68257']
                     gins1	['Q54HR6', 'Q7ZT47', 'Q14691', 'A4IFH4', 'Q9CZ15']
                     fbx29	['Q8N3Y1']
                    hisn6a	['B9DHD3']
                  hzs-beta	['Q1Q0T4']
                     arc19	['O96625', 'P33204']
                  aug-beta	['Q6UXT8']
                     aacc1	['P23181']
                  atabcg34	['Q7PC87']
      alpha-1,2-fucosidase	['Q9FXE5']
                    actr10	['Q3ZBD2', 'Q9NZ32', 'Q54JY2', 'Q9QZB7']
                 aug-alpha	['Q6UX46']
                     uba52	['P62984', 'P62986', 'P63053', 'P0C275', 'P0C276', 'P63048', 'P63050', 'P62985', 'P63052', 'P62987', 'P0C273']
                 c20orf196	['Q8IYI0']
                    cldn16	['Q9XT98', 'Q9Y5I7', 'Q925N4', 'Q91Y55']
                     cpk30	['Q9SSF8']
                     cyp20	['O43447']
                     znrf3	['A5WWA0', 'Q4KLR8', 'Q08D68', 'Q9ULT6', 'Q5SSZ7']
                     pyl12	['Q9FJ49']
                     pnp-a	['Q9ZV52']
                     bat1a	['Q9Z1N5', 'Q63413']
                     bm-12	['Q9UAD0']
                   b4galt4	['Q80WN7', 'O60513', 'Q9JJ04', 'Q66HH1']
                  beta-pix	['Q14155', 'Q9ES28', 'O55043']
                     rlp44	['Q9M2Y3']
                     rnf43	['Q68DV7', 'Q5NCP0', 'P0DPR2']
                    slc1a4	['A2VDL4', 'P43007', 'O35874']
                   slc30a3	['Q08E25', 'Q99726', 'Q5R617', 'P97441', 'Q6QIX3']
                   slc35a3	['Q6YC49', 'O77592', 'Q9Y2D2', 'Q8R1T4', 'Q6AXR5']
                   slc35d2	['A2VE55', 'Q76EJ3', 'Q15B89', 'Q5RDC9', 'Q762D5']
                     sms-3	['Q965Q4']
                     sec1b	['Q9SZ77']
 endo-1,6-beta-d-glucanase	['Q7M4T0']
                     dna-c	['Q65389', 'Q9WIJ4', 'O91253', 'Q9Z0D2', 'Q87010']
                     dtx35	['F4JTB3']
                    mgat4b	['Q6GMK0', 'Q9UQ53', 'Q812F8']
                 mind-bomb	['Q9VUX2']
                     med31	['P0CS74', 'Q6BUN9', 'Q8IH24', 'Q8SU76', 'Q9Y3C7', 'Q6CUD2', 'Q0U957', 'Q4P308', 'Q6CAU9', 'P38633', 'Q17DI7', 'Q8VYB1', 'Q75EB3', 'A0JNN3', 'Q59P87', 'Q6FR78', 'Q1E146', 'P0CS75', 'Q6DH26', 'Q54NI7', 'Q5BAD5', 'A1D619', 'Q7SF81', 'Q9USH1', 'Q28FE2', 'A1CL91', 'Q4WYS1', 'A2QU71', 'Q0CQK7', 'Q9CXU1']
                     mfe-2	['P51659', 'P51660', 'P97852', 'Q54XZ0']
                     nptii	['P00552']
                     nhej1	['Q3KNJ2', 'Q6AYI4', 'Q6NV18', 'Q9H9Q4']
                     trp63	['O88898', 'Q9JJP6']
l-isoleucine-4-hydroxylase	['E2GIN1']
            l-carbamoylase	['P37113', 'Q53389']
                     ilp-2	['Q95M71', 'Q96P09', 'Q95M72']
               ifn-alpha-1	['P01572', 'P49879']
                     vc1.1	['P69747']
                    v-erbb	['P00534', 'P00535', 'P11273']
                    orf426	['P58151', 'Q70LD6']

"""
import pickle
from itertools import chain
from collections import Counter
from multiprocessing import Pool
from Papers import Journal
from mypathlib import PathTemplate


_R_FILE = PathTemplate('$rsrc/pdata/paper/matched/$journal.pkl.gz')
W_FILES = {'counts': PathTemplate('$rsrc/lite/match/match_counts.pkl').substitute(),
           'paths': PathTemplate('$rsrc/lite/match/matched_paths.pkl').substitute(),
           'double_check': PathTemplate('$rsrc/lite/match/matched_double_check.pkl').substitute()}


def get_matched_texts(j):
    res = {}
    c = Counter()
    for matches in j.get_matches().values():
        for match in chain(*matches):
            c[match.text] += 1
            res[match.text] = match.tokens
    return c, res, j


def _main(js):
    _counts = Counter()
    _paths = {}
    _double_check = []
    for j in js:
        _count, _path, _j = get_matched_texts(j)
        _counts += _count
        _paths.update(_path)
        _double_check.append(_j)
    return _counts, _paths, _double_check


def main():
    counts = Counter()
    paths = {}
    args = Journal.journals4mp(50, selected=False)

    double_check = []
    with Pool(50) as p:
        for count, path, check in p.map(_main, args):
            counts += count
            paths.update(path)
            double_check.append(check)

    with open(W_FILES['counts'], 'wb') as file:
        pickle.dump(counts, file)
    with open(W_FILES['paths'], 'wb') as file:
        pickle.dump(paths, file)
    with open(W_FILES['double_check'], 'wb') as file:
        pickle.dump(double_check, file)


if __name__ == '__main__':
    main()
