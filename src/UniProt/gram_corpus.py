import pickle
from UniProt.containers import Nested, KeyWord


with open('./uniprot_keywords.pkl', 'rb') as file:
    keywords: list[KeyWord] = pickle.load(file)
print(f"uniprot_keyw_list : {len(keywords)}")

# unip = Unip()  # {possible ngram -> idx}
# unif = Unif()  # {ngram path -> joined ngrams}
nested = Nested()  # ngram path dictionary
for kw in keywords:
    if kw.is_valid():
        for tokens, joined in kw.get_all_tokens():
            lower_tokens = tuple(token.lower() for token in tokens)
            # unip.extend(lower_tokens)
            nested.extend(lower_tokens, joined)
            # unif.extend(lower_tokens, joined)

# print(len(unip), ": unip")
# print(len(unif), ": unif")
print(len(nested), ": nested")
print("UniProt gram corpus loaded")
with open('./nested.pkl', 'wb') as file:
    pickle.dump(nested, file)

# uniprot_keyw_list : 941826
# 149068 : unip
# 1211609 : unif
# 72863 : nested
# UniProt gram corpus loaded
