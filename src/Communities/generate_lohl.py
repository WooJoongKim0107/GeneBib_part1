import pickle
from mypathlib import PathTemplate
from Communities import Community
Community.import_cache_if_empty()


R_FILES0 = {'paper': PathTemplate('$lite/paper/pmid2year.pkl').substitute(),
            'us_patent': PathTemplate('$lite/us_patent/pubnum2year.pkl').substitute(),
            'us_patent_gon': PathTemplate('$lite/us_patent/pubnum2year.pkl').substitute(),
            'cn_patent': PathTemplate('$lite/cn_patent/pubnum2year.pkl').substitute(),
            'cn_patent_gon': PathTemplate('$lite/cn_patent/pubnum2year.pkl').substitute(),
            'ep_patent': PathTemplate('$lite/ep_patent/pubnum2year.pkl').substitute(),
            'ep_patent_gon': PathTemplate('$lite/ep_patent/pubnum2year.pkl').substitute(),}
R_FILES1 = {'paper': PathTemplate('$lite/paper/pmid2cmnt.pkl').substitute(),
            'us_patent': PathTemplate('$lite/us_patent/pubnum2cmnt.pkl').substitute(),
            'us_patent_gon': PathTemplate('$lite/us_patent/pubnum2cmnt.pkl').substitute(),
            'cn_patent': PathTemplate('$lite/cn_patent/pubnum2cmnt.pkl').substitute(),
            'cn_patent_gon': PathTemplate('$lite/cn_patent/pubnum2cmnt.pkl').substitute(),
            'ep_patent': PathTemplate('$lite/ep_patent/pubnum2cmnt.pkl').substitute(),
            'ep_patent_gon': PathTemplate('$lite/ep_patent/pubnum2cmnt.pkl').substitute(),}
R_FILES2 = {'us_patent': PathTemplate('$lite/us_patent/granted.pkl').substitute(),
            'cn_patent': PathTemplate('$lite/cn_patent/granted.pkl').substitute(),
            'ep_patent': PathTemplate('$lite/ep_patent/granted.pkl').substitute(),}
W_FILES = {'paper': PathTemplate('$pdata/to_part2/paper_hitgene_list.txt').substitute(),
           'us_patent': PathTemplate('$pdata/to_part2/us_patent_hitgene_list.txt').substitute(),
           'us_patent_gon': PathTemplate('$pdata/to_part2/us_patent_hitgene_list_gon.txt').substitute(),
           'cn_patent': PathTemplate('$pdata/to_part2/cn_patent_hitgene_list.txt').substitute(),
           'cn_patent_gon': PathTemplate('$pdata/to_part2/cn_patent_hitgene_list_gon.txt').substitute(),
           'ep_patent': PathTemplate('$pdata/to_part2/ep_patent_hitgene_list.txt').substitute(),
           'ep_patent_gon': PathTemplate('$pdata/to_part2/ep_patent_hitgene_list_gon.txt').substitute(),}


def write(mtype: str):
    with open(W_FILES[mtype], 'wt') as file:
        file.write('real_key,year,cmnt_idx...\n')
        for x in generate(mtype):
            file.write(','.join(map(str, x)))
            file.write('\n')


def generate(mtype: str):
    with open(R_FILES0[mtype], 'rb') as file:
        pmid2year = pickle.load(file)
    with open(R_FILES1[mtype], 'rb') as file:
        pmid2cmnt = pickle.load(file)

    it = ((pmid, year) for pmid, year in pmid2year.items())  # No recode without valid year - by the construction
    it = ((pmid, year, *pmid2cmnt[pmid]) for pmid, year in it if pmid in pmid2cmnt)  # No recode without hit cmnt
    if mtype in ['us_patent', 'cn_patent', 'ep_patent']:
        with open(R_FILES2[mtype], 'rb') as file:
            granted = {pubnum for pubnum, isgranted in pickle.load(file).items() if isgranted}
        # Only granted patents
        it = ((pmid, year, *cmnts) for pmid, year, *cmnts in it if pmid in granted)
    yield from it


def main():
    for mtype in ['paper',
                  'us_patent', 'us_patent_gon',
                  'cn_patent', 'cn_patent_gon',
                  'ep_patent', 'ep_patent_gon',]:
        write(mtype)


if __name__ == '__main__':
    main()
