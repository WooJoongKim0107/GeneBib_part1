from collections import Counter
import pandas as pd
from Communities import Community
from mypathlib import PathTemplate


Community.import_cache()
_R_FILE = PathTemplate('$pdata/community/community_cache.pkl.gz').substitute()
_PUB_YEAR_PATHS = {'paper': PathTemplate('$lite/paper/pmid2year.pkl').substitute(),
                   'us_patent': PathTemplate('$lite/us_patent/pubnum2year.pkl').substitute()}
W_FILE = PathTemplate('$pdata/to_part2/yearly_new_genes.csv').substitute()


def get():
    c = Counter()
    for v in Community.values():
        (_, _, p_year), (_, _, t_year), _ = v.get_first(load=True)  # We don't use EPO dataset here
        if (year := min(p_year, t_year)) != 9999:  # year==9999 indicates that the Community has no valid publication
            c[year] += 1
    return c


def sort_and_fill(dct):
    k_min, k_max = min(dct), max(dct)
    return {k: dct.get(k, 0) for k in range(k_min, k_max+1)}


def main():
    c = sort_and_fill(get())
    df = pd.DataFrame(data=c.values(), index=c.keys(), columns=['new_entries'])
    df.rename_axis('year', inplace=True)
    df.to_csv(W_FILE)


if __name__ == '__main__':
    main()
