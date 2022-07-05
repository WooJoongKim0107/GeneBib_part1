from StringMatching.base import uniform_match, get_ngram_list3
from UniProt.gram_corpus import unip, unif, nested


def gram_based_match2(target_text, target_code):
    target_match_list = bar(target_text, target_code)
    target_match_list.sort(key=_sort_key)
    filter_smaller(target_match_list)
    return target_match_list


def bar(target_text, target_code):
    target_match_list = []
    ngrams, indices = get_ngram_list3(target_text)
    codes = tuple(unip.get(ngram.lower()) for ngram in ngrams)
    for i, idx in enumerate(indices):  # i=ngram index, idx=location on target_text
        for matched_gram_tuple in poo(codes[i:]):
            candid_keyw_set = unif[matched_gram_tuple]
            unif_gram_concat = uniform_match(''.join(ngrams[i:i + len(matched_gram_tuple)]))
            if unif_gram_concat in candid_keyw_set:
                end = indices[i + len(matched_gram_tuple) - 1] + len(ngrams[i + len(matched_gram_tuple) - 1])
                target_Keyw = target_text[idx:end]  # 마지막 index 확인
                match = (target_code, idx, target_Keyw, matched_gram_tuple)
                target_match_list.append(match)
    return target_match_list


def poo(codes):
    """return True if match exist, False otherwise"""
    i = 0
    level = nested
    while i < len(codes) and (code := codes[i]) in level:
        i += 1
        level = level[code]
        if -1 in level:
            yield codes[:i]


def _sort_key(match):
    return match[1], -len(match[2])


def location(match):
    return match[1], match[1] + len(match[2])


def filter_smaller(matches):
    initial, final, i = 0, -1, 0
    while i < len(matches):
        ini, fin = location(matches[i])
        if (initial <= ini) and (fin <= final):
            matches.pop(i)
        else:
            initial, final = ini, fin
            i += 1
