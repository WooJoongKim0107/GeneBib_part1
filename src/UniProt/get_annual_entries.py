from collections import Counter
import pandas as pd
from UniProt import Entry
from mypathlib import PathTemplate


Entry.import_cache()
_R_FILE = PathTemplate('$pdata/uniprot/uniprot_sprot_parsed.pkl.gz').substitute()
W_FILE = PathTemplate('$pdata/to_part2/yearly_new_entries.csv').substitute()


def get():
    c = Counter()
    for v in Entry.values():
        if (year := v.first_pub_year) != 9999:  # year==9999 indicates that the Entry has no valid publication
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
