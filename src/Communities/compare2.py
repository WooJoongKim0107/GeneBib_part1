import pickle
from Communities import Community


with open('./old_paper_by_ranks.pkl', 'rb') as file:
    p = pickle.load(file)
Community.import_cache()
cmnts = sorted(Community.values(), key=lambda x: x.total_paper_hits, reverse=True)


def in_old(cmnt_idx):
    for i, idx in enumerate(p):
        if idx == cmnt_idx:
            return i, p[idx]
    return -1, 0


def in_new(cmnt_idx):
    for i, c in enumerate(cmnts):
        if c.cmnt_idx == cmnt_idx:
            return i, c.total_paper_hits
    return -1, 0


def compare(x, exclude_new=None, exclude_old=None):
    exclude_new = set() if exclude_new is None else exclude_new
    exclude_old = set() if exclude_old is None else exclude_old

    new_rankers = {c.cmnt_idx for c in cmnts[:x] if c.cmnt_idx not in exclude_new}
    old_rankers = {idx for idx in list(p)[:x] if idx not in exclude_old}

    only_news = new_rankers.difference(old_rankers)
    only_olds = old_rankers.difference(new_rankers)

    for idx in only_news:
        ornk, oh = in_old(idx)
        nrnk, nh = in_new(idx)
        print(f'\n{ornk}th -> {nrnk}th   {oh} -> {nh}')
        print(Community[idx].less_info)
    print('-----------------------------')
    for idx in only_olds:
        ornk, oh = in_old(idx)
        nrnk, nh = in_new(idx)
        print(f'\n{ornk}th -> {nrnk}th   {oh} -> {nh}')
        print(Community[idx].less_info)
