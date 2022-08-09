# -*- coding:utf-8 -*-

'''
python ver. 3.6.12

Declare function for gram-based text match

input : target_text, uniprot_gram_corpus
output : target_match_list

'''

from WC_StringMatching.uniprot_gram_corpus import get_ngram_list3, uniform_match, \
    nested_keyw_dict, unip_unigram_dict, unif_gram_concat_dict
from mypathlib import PathTemplate
''' including 
  're'
  'pickle'
  'os.getcwd'
  'datetime.datetime'
  'np.binary_repr'

  'uniprot_keyw_list'
  'unip_unigram_dict'
  'unif_gram_concat_dict'
  'nested_keyw_dict'
'''

print(f'{"="*80}')
print(f"imported : {__file__}")
print(f"namespace : {__name__}")
# below line works when 'keyword_mode' is imported with '*'.
# print(f"cwd : {getcwd()}")
# print(f"greek_dict : {len(greek_dict)}")

proj_root = PathTemplate('$base/src/WC_StringMatching').substitute()


def gram_based_match(target_text, target_code):
    '''
    Take target text as an input, return a list of match entries containing
    information of matched grams and location of hit phrase inside the text. 
    'unip_unigram_dict', 'unif_gram_concat_dict' and 'nested_keyw_dict' need
    to be declared in advance.
    '''
    # convert the text of title and abstrct into sequence of gram
    # target_match_list = get_ngram_list3(target_text)
    target_gram_result = get_ngram_list3(target_text)
    target_gram_list = target_gram_result[0]
    target_match_list = []
    
    # loop over gram sequence for the title text in a given patent article
    for target_gram_idx, gram_i in enumerate(target_gram_list) :
        level_gram_dict = nested_keyw_dict
        matched_gram_list = []
        lower_gram = gram_i.lower()
        gram_start_point = target_gram_result[1][target_gram_idx]
        
        if not lower_gram in unip_unigram_dict : continue
        
        encoded_gram = unip_unigram_dict[lower_gram]
        next_gram_idx = target_gram_idx
        seq_idx = 0
        
        while True :
          if encoded_gram in level_gram_dict :
            level_gram_dict = level_gram_dict[encoded_gram]
            matched_gram_list.append(encoded_gram)
            seq_idx += 1
            
            if -1 in level_gram_dict :
              matched_gram_tuple = tuple(matched_gram_list)
              candid_keyw_set = unif_gram_concat_dict[matched_gram_tuple]
              unif_gram_concat = uniform_match(''.join(
                        target_gram_list[target_gram_idx:target_gram_idx+seq_idx]))
              
              if unif_gram_concat in candid_keyw_set :
                target_Keyw = target_text[ gram_start_point : target_gram_result[1][
                              target_gram_idx+seq_idx-1] + len(lower_gram) ]
                target_match_list.append(
                      (target_code, gram_start_point, target_Keyw, matched_gram_tuple))
            
            next_gram_idx += 1
            
            if next_gram_idx == len(target_gram_list) : break
            
            next_gram = target_gram_list[next_gram_idx]
            lower_gram = next_gram.lower()
            
            if not lower_gram in unip_unigram_dict : break
            
            encoded_gram = unip_unigram_dict[lower_gram]
          
          else : break
    
    # target_match_list.append((0, gram_start_point, target_Keyw, matched_gram_tuple))
    # remove matches included in other matches
    
    # sort by keyword length
    target_match_list.sort(reverse=True, key=lambda m: len(m[2]))
    # sort by start point
    target_match_list.sort(key=lambda m: m[1])
    
    for match_idx, match_i in enumerate(target_match_list) :
        match_start_i = match_i[1]
        match_end_i = match_start_i+len(match_i[2])
        match_idx_j = match_idx + 1
        
        while match_idx_j < len(target_match_list) :
            match_j = target_match_list[match_idx_j]
            match_start_j = match_j[1]
            match_end_j = match_start_j+len(match_j[2])
            
            if match_start_j < match_end_i :
                if match_end_j <= match_end_i :
                    del target_match_list[match_idx_j]
                
                else: match_idx_j += 1
            else: match_idx_j += 1
    
    return target_match_list

