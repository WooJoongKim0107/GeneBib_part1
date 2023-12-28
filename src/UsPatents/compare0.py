import pickle
from more_itertools import nth
from UniProt import KeyWord
from Communities import Community, C2E, C2K

Community.import_cache()
with open('./rsrc/data/cmnt_hit_dict_210113_gon.pyc', 'rb') as file:
    q = pickle.load(file)
c2e = C2E.load()
c2k = C2K.load()
keywords = KeyWord.load_k2k()


def get_old(cmnt):
    if cmnt not in q:
        return {}
    else:
        return {f'US-{idx}-{typ}' for *_, (idx, typ) in q[cmnt]}


def get_new(cmnt):
    if cmnt not in Community:
        return {}
    else:
        return set().union(*Community[cmnt].pub_numbers.values())


cmnt = nth(q, 3)
old, new = get_old(cmnt), get_new(cmnt)
old.difference(new)
