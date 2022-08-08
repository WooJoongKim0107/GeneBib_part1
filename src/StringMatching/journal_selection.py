import pickle
from Papers import Journal

with open('./jcr_journals.pkl', 'rb') as file:
    jcrs = {k.lower(): v for k, v in pickle.load(file).items()}

with open('./field2jnl.pkl', 'rb') as file:
    f2j = pickle.load(file)


def foo(j):
    match jcrs.get(j.lower(), {}):
        case {'ISSN': str(issn)} if res := list(Journal.find_issn(issn)):
            return 'ISSN', res
        case {'eISSN': str(issn)} if res := list(Journal.find_issn(issn)):
            return 'eISSN', res

    if res := list(Journal.find_title(j, strict=True)):
        return 'strict', res
    else:
        # There are two cases ('Algae', 'Sociobiology')
        #  where list(Journal.find_title(j, strict=False)) is not empty.
        # However, those results seems not proper.
        return 'failed', []


q = {}
for js in f2j.values():
    for j in js:
        if j not in q:
            q[j] = foo(j)
for j, (method, result) in q.items():
    assert len(result) == 1 or method == 'failed'
selected = {j: result[0].medline_ta for j, (method, result) in q.items() if method != 'failed'}

with open('./jnls_selected.pkl', 'wb') as file:
    pickle.dump(selected, file)
