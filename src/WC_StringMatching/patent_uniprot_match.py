# -*- coding:utf-8 -*- 

'''
python ver. 3.6.12

Execute match between UniProt keywords and patent text
Use token indexed full-text match method

input : '.json' text file of patents published in US
output : match result entries in python list in pickled format

'''

# from src.gram_based_match import gram_based_match
from WC_StringMatching.gram_based_match import gram_based_match
# from gram_based_match import *
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

import os
import gzip
import pickle
import json
from datetime import datetime
from multiprocessing import Pool
from mypathlib import PathTemplate

proj_root = PathTemplate('$base/src/WC_StringMatching').substitute()
starting_time = datetime.now()


# out_dir_path = os.path.join(proj_root,"data/test_uniprot_match/")
out_dir_path = os.path.join(proj_root,"data/updated_unip_pat_match/")

# patent_dir_path = '/home/data/wcjung/patent/us_raw_200920'
patent_dir_path = '/home/data/wcjung/patent/us_raw_220429'
patent_path_list = []
for root, d_names, f_names in os.walk(patent_dir_path):
    for f in f_names:
        if f.startswith("us_patent"):
            patent_path_list.append(os.path.join(root,f))

patent_path_list.sort()
print("%d patent file pathes found."%len(patent_path_list))


def patent_uniprot_match(patent_path_idx) :
    '''
    The match execution function for multiprocessing call.
    It takes integer index of patent file inside the path list, and returns
    tuples containing match counts.
    The function export intact match information as pickled files into the
    predefined output directory.
    '''
    patent_path = patent_path_list[patent_path_idx]
    
    try:
        patent_file = gzip.open(patent_path, 'rt')
        first_line = patent_file.readline()
    except gzip.BadGzipFile:
        patent_file.close()
        patent_file = open(patent_path, 'r')
    
    patent_file.seek(0)
    match_total_count = 0
    match_patent_count = 0
    match_list = []
    truncated_list = []
    
    # loop over .json files containing patent data
    for patent_idx, entry_line in enumerate(patent_file) :
      
      # check any broken .json file
      try :
          patent_entry = json.loads(entry_line)
      except json.decoder.JSONDecodeError:
          truncated_list.append(entry_line)
          continue
      
      # load title and abstrct from the patent articel entry
      ttl_text = ""
      abs_text = ""
      if patent_entry['title_localized']:
          ttl_text_list = patent_entry['title_localized']
          for ttl_text_entry in ttl_text_list :
              if ttl_text_entry['language'] == 'en' :
                  ttl_text = ttl_text_entry['text']
      
      if patent_entry['abstract_localized']:
          abs_text_list = patent_entry['abstract_localized']
          for abs_text_entry in abs_text_list :
              if abs_text_entry['language'] == 'en' :
                  abs_text = abs_text_entry['text']
      
      # call match execution function
      ttl_match_list = gram_based_match(ttl_text, 0)
      abs_match_list = gram_based_match(abs_text, 1)
      
      patent_match_list = ttl_match_list+abs_match_list
      match_total_count += len(patent_match_list)
      
      if patent_match_list:
          match_list.append(((patent_path_idx, patent_idx), patent_match_list))
          match_patent_count += 1
    
    # export match result entries in python list into pickled files
    with open(out_dir_path+'/test_match_210707_01_%04d.pyc' %patent_path_idx,
              'wb') as match_out_file:
      pickle.dump(match_list, match_out_file)
    
    patent_file.close()
    
    print(f'file No. {patent_path_idx:4d} done; {len(match_list)} matches')
    
    return((match_patent_count, match_total_count, truncated_list))


os.makedirs(out_dir_path, exist_ok=True)

ending_time = datetime.now()
starting_time = ending_time


processor_num = 4
processing_list_size = len(patent_path_list[:])
chunk_size = int(processing_list_size/processor_num)+1
begin_idx = 100
total_match_patent_count = 0
total_match_total_count = 0
total_truncated_list = []

with Pool(processor_num) as text_match_Pool :
    ovLookResultIter = text_match_Pool.imap_unordered( patent_uniprot_match,
                                      range(begin_idx, processing_list_size) )
    for resultArray in ovLookResultIter :
        # total_match_list += resultArray[0]
        total_match_patent_count += resultArray[0]
        total_match_total_count += resultArray[1]
        total_truncated_list += resultArray[2]

print("%d total match hit patent" %total_match_patent_count)
print("%d total match hit count" %total_match_total_count)
print("%d total truncated text count" %len(total_truncated_list))

ending_time = datetime.now()
print(f"\n{str(ending_time-starting_time)} elapsed")
starting_time = ending_time
print("UniProt match on patent text completed")
