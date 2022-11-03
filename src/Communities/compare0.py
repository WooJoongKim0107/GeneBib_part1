import pickle
from mypathlib import PathTemplate
from Communities import Community, C2K


Community.import_cache()
c2k = C2K.load()
old = C2K.load(0)
with open(PathTemplate('$base/old_paper_by_ranks.pkl').substitute(), 'rb') as file:
    old_ps = pickle.load(file)
with open(PathTemplate('$base/old_patent_by_ranks.pkl').substitute(), 'rb') as file:
    old_ts = pickle.load(file)


def foo(x):
    a = old.get(x, set())
    b = c2k.get(x, set())
    return f'Only in old: {a.difference(b)}\nOnly in new: {b.difference(a)}'


def sorted_dict(dct):
    return dict(sorted(dct.items(), key=lambda x: x[1], reverse=True))


ps = sorted_dict({idx: c.total_paper_hits for idx, c in Community.items()})
with open(PathTemplate('$base/top_papered_cmnts.txt').substitute(), 'w') as file:
    j = 0
    for i, (k, v) in enumerate(old_ps.items()):
        if j > 100:
            break
        if ps.get(k, 0) < v:
            j += 1
            file.write(f'{i}th in old paper hits:: ({v} -> {ps.get(k, None)})\n')
            if k in Community:
                file.write(foo(k))
                file.write('\n')
                file.write(Community[k].more_info)
                file.write('=============================\n\n')
            else:
                file.write(f'Community({k}) does not exist!!!\n\n')

ts = sorted_dict({idx: c.total_patent_hits for idx, c in Community.items()})
with open(PathTemplate('$base/top_patented_cmnts.txt').substitute(), 'w') as file:
    j = 0
    for i, (k, v) in enumerate(old_ts.items()):
        if j > 100:
            break
        if ts.get(k, 0) < v:
            j += 1
            file.write(f'{i}th in old patent hits:: ({v} -> {ts.get(k, None)})\n')
            if k in Community:
                file.write(foo(k))
                file.write('\n')
                file.write(Community[k].more_info)
                file.write('=============================\n\n')
            else:
                file.write(f'Community({k}) does not exist!!!\n\n')
