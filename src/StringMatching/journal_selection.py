import pickle
import pandas as pd
from Papers import Journal

journals = pd.read_csv('./wos-core_SCIE_2022-April-19_selected.csv', header=0, index_col=0)
journals = journals.T.to_dict()


def foo(k, v):
    match v:
        case {'ISSN': str(issn)} if res := list(Journal.find_issn(issn)):
            return 'ISSN', res
        case {'eISSN': str(issn)} if res := list(Journal.find_issn(issn)):
            return 'eISSN', res
    if res := list(Journal.find_title(k, strict=True)):
        return 'title', res
    else:
        # There are 9 cases ('Algae', 'Sociobiology')
        #  where list(Journal.find_title(j, strict=False)) is not empty.
        # However, those results seems not proper.
        return 'failed', []


q = {k: foo(k, v) for k, v in journals.items()}
# Counter({'ISSN': 3696, 'eISSN': 81, 'title': 8, 'failed': 247})
# Counter({'len(res)==1': 3785, 'len(res)==0': 247})
for k, (method, result) in q.items():
    assert len(result) == 1 or method == 'failed'
selected = {k: result[0].medline_ta for k, (method, result) in q.items() if method != 'failed'}

with open('./jnls_selected.pkl', 'wb') as file:
    pickle.dump(selected, file)
