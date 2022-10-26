from UniProt import KeyWord
from Communities import Community


keywords = {k: k for k in KeyWord.load()}
Community.import_cache()


def split_off(x):
    lines = (line for line in x.splitlines())
    lines = (line.partition('#')[0].rstrip() for line in lines)
    lines = (line.partition(';')[0].rstrip() for line in lines)
    yield from (line for line in lines if line)


def build(x):
    res = {}
    cmnt_idx, values = 0, []

    for line in split_off(x):
        if line.startswith('Cmnt No.'):
            if values:
                res[cmnt_idx] = values
            cmnt_idx = int(line[8:].partition('with')[0].strip())
            values = []
        else:
            values.append(line)
    return res


def if_keys_added(s):
    res = build(s)
    for cmnt_idx, ks in res.items():
        if cmnt_idx not in Community:
            print(f'{cmnt_idx}: not constructed')
        else:
            c = Community[cmnt_idx]
            keys = {keywords[k] for k in ks}
            if not keys.issubset(c.keywords):
                print(f'{cmnt_idx}: {keys.symmetric_difference(c.keywords)}')


def if_keys_removed(ss):
    ress = build(ss)
    for cmnt_idx, ks in ress.items():
        if cmnt_idx not in Community:
            print(f'{cmnt_idx}: not constructed')
        else:
            c = Community[cmnt_idx]
            keys = {keywords[k] for k in ks if k in keywords}
            if not keys.isdisjoint(c.keywords):
                print(f'{cmnt_idx}: {keys.intersection(c.keywords)}')
