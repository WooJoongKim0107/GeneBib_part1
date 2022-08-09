# -*- coding:utf-8 -*-

'''
python ver. 3.6.12 

Reconstruct the corpus of keyword grams into sequentially nested dictionary.
Consider redundancy for gram identification. cf) Stemming, Lemmatization

input : linsted keyword from Swiss-prot raw data; "keyword_list_200201.pyc"
output :

'''
import re
from mypathlib import PathTemplate
from WC_StringMatching.keyw_modify.keyword_mode import get_ngram_list3, uniform_match, plural_keyw

import pickle
from datetime import datetime
from numpy import binary_repr

print(f'{"="*80}')
print(f"imported : {__file__}")
print(f"namespace : {__name__}")
# print(f"cwd : {getcwd()}")
# below line works when 'keyword_mode' is imported with '*'.
# print(f"greek_dict : {len(greek_dict)}")

proj_root = PathTemplate('$base/src/WC_StringMatching').substitute()
starting_time = datetime.now()


alph2greek_dict = {
"alpha" :    '\u03b1',  # α, Α
"beta" :     '\u03b2',  # β, Β
"gamma" :    '\u03b3',  # γ, Γ
"delta" :    '\u03b4',  # δ, Δ
"epsilon" :  '\u03b5',  # ε, Ε
"zeta" :     '\u03b6',  # ζ, Ζ
"eta" :  '\u03b7',  # η, Η
"iota" :     '\u03b9',  # ι, Ι
"kappa" :    '\u03ba',  # κ, Κ
"lambda" :    '\u03bb',  # λ, Λ
"mu" :   '\u03bc',  # μ, Μ
"nu" :   '\u03bd',  # ν, Ν
"xi" :   '\u03be',  # ξ, Ξ
"omicron" :  '\u03bf',  # ο, Ο
"pi" :   '\u03c0',  # π, Π
"rho" :  '\u03c1',  # ρ, Ρ
"tau" :  '\u03c4',  # τ, Τ
"upsilon" :  '\u03c5',  # υ, Υ
"phi" :  '\u03c6',  # φ, Φ
"chi" :  '\u03c7',  # χ, Χ
"psi" :  '\u03c8',  # ψ, Ψ
"omega" :    '\u03c9',  # ω, Ω
"theta" :    '\u03b8', # θ, Θ, symbol theta
"sigma" :    '\u03c3'} # σ, Σ, ς(final sigma)


# Uni_prot Keywords preparation step
# with open(f"{proj_root}/data/uniprot_xml_parse/keyword_list_200201.pickle",
with open(f"{proj_root}/data/uniprot_xml_parse/added_keyw_2022-05_2019-01.pkl",
            'rb') as uniprot_keyw_file:
    uniprot_keyw_list = pickle.load(uniprot_keyw_file)

print(f"uniprot_keyw_list : {len(uniprot_keyw_list)}")

unip_unigram_dict = dict()
unif_gram_concat_dict = dict()
nested_keyw_dict = dict()
unigram_idx = 0
unigram_list = []

for keyw_idx, uni_pkeyword in enumerate(uniprot_keyw_list) :
    ngram_list = get_ngram_list3(uni_pkeyword[0])
    code = binary_repr(uni_pkeyword[1], width=8)
    
    # add plural name in case of protein description keyword
    neg_idx = 0
    if code[0] == '0':
        for gram_num, a_gram in enumerate(reversed(ngram_list[0])) :
            lower_gram = a_gram.lower()
            plural_idx = -(gram_num)
            if re.fullmatch("\w*protein$", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w*enzyme$", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w*subunit$", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w*isoform", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w*peptdie", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w*chain", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w{3,}log$", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w{3,}[oe]r$", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w{3,}gen", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w{3,}ase", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w{3,}ant", lower_gram) : neg_idx = plural_idx; break
            elif re.fullmatch("\w{2,}xin", lower_gram) : neg_idx = plural_idx; break
        neg_idx += -1
    greek_in_keyw = 0
    level_dict = nested_keyw_dict
    gram_idx_seq = []
    gram_concat = ''
    
    for gram_num, a_gram in enumerate(ngram_list[0]) :
        gram_concat += a_gram
        lower_gram = a_gram.lower()
        if not a_gram in unip_unigram_dict :
            gram_idx = unigram_idx
            unigram_idx += 1
            unip_unigram_dict[a_gram] = gram_idx
            unigram_list.append(a_gram)
        if lower_gram in alph2greek_dict : greek_in_keyw = 1
        if lower_gram in unip_unigram_dict :
            gram_idx = unip_unigram_dict[lower_gram]
        else :
            gram_idx = unigram_idx
            unigram_idx += 1
            unip_unigram_dict[lower_gram] = gram_idx
            unigram_list.append(lower_gram)
        level_dict = level_dict.setdefault(gram_idx, dict())
        gram_idx_seq.append(gram_idx)
    
    level_dict[-1] = 0
    unif_gram_concat_dict.setdefault(tuple(gram_idx_seq), set()).add(uniform_match(gram_concat))
    
    if neg_idx :  # plural form alt keyword
        plural_idx = len(ngram_list[0])+neg_idx
        level_dict = nested_keyw_dict
        gram_idx_seq = []
        gram_concat = ''
        
        for gram_num, a_gram in enumerate(ngram_list[0]) :
            if gram_num == plural_idx :
                a_gram = plural_keyw(a_gram)
            if not a_gram in unip_unigram_dict :  # links for case retained gram
                gram_idx = unigram_idx
                unigram_idx += 1
                unip_unigram_dict[a_gram] = gram_idx
                unigram_list.append(a_gram)
            gram_concat += a_gram
            lower_gram = a_gram.lower()
            if lower_gram in unip_unigram_dict :
                gram_idx = unip_unigram_dict[lower_gram]
            else :
                gram_idx = unigram_idx
                unigram_idx += 1
                unip_unigram_dict[lower_gram] = gram_idx
                unigram_list.append(lower_gram)
            level_dict = level_dict.setdefault(gram_idx, dict())
            gram_idx_seq.append(gram_idx)
        
        level_dict[-1] = 0
        unif_gram_concat_dict.setdefault(tuple(gram_idx_seq), set()).add(uniform_match(gram_concat))
    
    if greek_in_keyw :  # greek alt keyword
        level_dict = nested_keyw_dict
        gram_idx_seq = []
        gram_concat = ''
        
        for gram_num, a_gram in enumerate(ngram_list[0]) :
            lower_gram = a_gram.lower()
            if lower_gram in alph2greek_dict : 
                lower_gram = alph2greek_dict[lower_gram]
                gram_concat += lower_gram
            else: gram_concat += a_gram
            if lower_gram in unip_unigram_dict :
                gram_idx = unip_unigram_dict[lower_gram]
            else :
                gram_idx = unigram_idx
                unigram_idx += 1
                unip_unigram_dict[lower_gram] = gram_idx
                unigram_list.append(lower_gram)
            level_dict = level_dict.setdefault(gram_idx, dict())
            gram_idx_seq.append(gram_idx)
        
        level_dict[-1] = 0
        unif_gram_concat_dict.setdefault(tuple(gram_idx_seq), set()).add(uniform_match(gram_concat))
        
        if neg_idx :  # plural form alt keyword
            plural_idx = len(ngram_list[0])+neg_idx
            level_dict = nested_keyw_dict
            gram_idx_seq = []
            gram_concat = ''
            
            for gram_num, a_gram in enumerate(ngram_list[0]) :
                lower_gram = a_gram.lower()
                if lower_gram in alph2greek_dict : 
                    lower_gram = alph2greek_dict[lower_gram]
                    if gram_num == plural_idx :
                        lower_gram = plural_keyw(lower_gram)
                    gram_concat += lower_gram
                elif gram_num == plural_idx :
                    gram_concat += plural_keyw(a_gram)
                    lower_gram = plural_keyw(a_gram.lower())
                else : gram_concat += a_gram
                if lower_gram in unip_unigram_dict :
                    gram_idx = unip_unigram_dict[lower_gram]
                else :
                    gram_idx = unigram_idx
                    unigram_idx += 1
                    unip_unigram_dict[lower_gram] = gram_idx
                    unigram_list.append(lower_gram)
                level_dict = level_dict.setdefault(gram_idx, dict())
                gram_idx_seq.append(gram_idx)
            
            level_dict[-1] = 0
            unif_gram_concat_dict.setdefault(tuple(gram_idx_seq), set()).add(uniform_match(gram_concat))

print(len(unip_unigram_dict), ": unip_unigram_dict")
print(len(unif_gram_concat_dict), ": unif_gram_concat_dict")
print(len(nested_keyw_dict), ": nested_keyw_dict")

ending_time = datetime.now()
print(str(ending_time - starting_time)+" elapsed")
print("UniProt gram corpus loaded")
starting_time = ending_time



if __name__ == "__main__":
    
    test_list = [
        ("Beta/alpha-amylase", 0),
        ("Beta/gamma crystallin domain-containing protein 1", 0),
        ("Beta-galactosidase",0),
    ]
    
    test_ttl_list = [
        "yow know what? β-galactosidase is so much delicious.",
        "yow know what? β-galactosidases is so much delicious.",
        "yow know what? beta-galactosidase is so much delicious.",
        "I need β/α-amylase",
        "14-3-3 protein β/α",
        "β/γ crystallin DOmain-containing proteins 1",
    ]    
    
    for ttl_text in test_ttl_list:
        ttl_gram_result = get_ngram_list3(ttl_text)
        ttl_gram_list = ttl_gram_result[0]
        ttl_match_list =[]
        for ttl_gram_idx, a_gram in enumerate(ttl_gram_list) :
            level_gram_dict = nested_keyw_dict
            matched_gram_list = []
            lower_gram = a_gram.lower()
            gram_start_point = ttl_gram_result[1][ttl_gram_idx]
            if not lower_gram in unip_unigram_dict : continue
            encoded_gram = unip_unigram_dict[lower_gram]
            next_gram_idx = ttl_gram_idx
            seq_idx = 0
            while True :
                if encoded_gram in level_gram_dict :
                    level_gram_dict = level_gram_dict[encoded_gram]
                    matched_gram_list.append(encoded_gram)
                    seq_idx += 1
                    if -1 in level_gram_dict :
                        matched_gram_tuple = tuple(matched_gram_list)
                        candiate_keyw_set = unif_gram_concat_dict[matched_gram_tuple]
                        unif_gram_concat = uniform_match(''.join(ttl_gram_list[ttl_gram_idx:ttl_gram_idx+seq_idx]))
                        if unif_gram_concat in candiate_keyw_set :
                            ttl_keyw = ttl_text[gram_start_point:ttl_gram_result[1][ttl_gram_idx+seq_idx-1]+len(lower_gram)]
                            ttl_match_list.append((0, gram_start_point, ttl_keyw, matched_gram_tuple))
                    next_gram_idx += 1
                    if next_gram_idx == len(ttl_gram_list) : break
                    next_gram = ttl_gram_list[next_gram_idx]
                    lower_gram = next_gram.lower()
                    if not lower_gram in unip_unigram_dict : break
                    encoded_gram = unip_unigram_dict[lower_gram]
                else : break
    
    print (ttl_match_list)
