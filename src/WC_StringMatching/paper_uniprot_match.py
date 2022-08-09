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
import pickle
from datetime import datetime
from multiprocessing import Pool
from mypathlib import PathTemplate

proj_root = PathTemplate('$base/src/WC_StringMatching').substitute()
starting_time = datetime.now()


# out_dir_path = os.path.join(proj_root,"data/test_uniprot_match/")
out_dir_path = os.path.join(proj_root,"data/updated_unip_pap_match/")

# pyperFilePath = "../../../meMetrics3/data/pyperData_200517"
pyperFilePath = '/home/data/wcjung/pubmed/pickled_pubmed'
pyperPathList = []
for root, d_names, f_names in os.walk(pyperFilePath):
    for f in f_names:
        if f.startswith("ppr_"):
            pyperPathList.append(os.path.join(root,f))

pyperPathList.sort()
print (str(len(pyperPathList))+" files found")


def paper_uniprot_match(pyperPathIdx) :
    '''
    The match execution function for multiprocessing call.
    It takes integer index of paper file inside the path list, and returns
    tuples containing match counts.
    The function export intact match information as pickled files into the
    predefined output directory.
    '''
    with open(pyperPathList[pyperPathIdx], 'rb') as pyperFile:
      pyperDictList = pickle.load(pyperFile)
    
    matchTotalCount = 0
    matchPaperCount = 0
    matchList =[]
    removedMatchList = []
    
    for pyperIdx, pyperDict in enumerate(pyperDictList) :
      ttlText = ""
      absText = ""
      if 'TI' in pyperDict:
          ttlTextList = pyperDict['TI']
          for ttlTextEntry in ttlTextList :
              ttlText += ttlTextEntry
      if 'AB'in pyperDict:
          absTextList = pyperDict['AB']
          for absTextEntry in absTextList :
              absText += absTextEntry
      
      pubDate = pyperDict.get('DP', ['0'])      
      
      # call match execution function
      ttlMatchList = gram_based_match(ttlText, 0)
      absMatchList = gram_based_match(absText, 1)
      
      paperMatchList = ttlMatchList+absMatchList
      matchTotalCount += len(paperMatchList)
      
      if paperMatchList:
          matchList.append(((pyperPathIdx, pyperIdx), paperMatchList, pubDate[0]))
          matchPaperCount += 1
    
    # export match result entries in python list into pickled files
    with open(out_dir_path+'/test_match_210707_01_%04d.pkl' %pyperPathIdx,
              'wb') as match_out_file:
      pickle.dump(matchList, match_out_file)
    
    print(f'file No. {pyperPathIdx:4d} done; {len(matchList)} matched papers.')
    
    return((matchPaperCount, matchTotalCount,)) # truncated_list))


os.makedirs(out_dir_path, exist_ok=True)

ending_time = datetime.now()
starting_time = ending_time


processor_num = 4
processing_list_size = len(pyperPathList[:])
chunk_size = int(processing_list_size/processor_num)+1
begin_idx = 0
total_match_paper_count = 0
total_match_total_count = 0
total_truncated_list = []

with Pool(processor_num) as text_match_Pool :
    ovLookResultIter = text_match_Pool.imap_unordered( paper_uniprot_match,
                                      range(begin_idx, processing_list_size) )
    for resultArray in ovLookResultIter :
        total_match_paper_count += resultArray[0]
        total_match_total_count += resultArray[1]
        # total_truncated_list += resultArray[2]

print("%d total match hit paper" %total_match_paper_count)
print("%d total match hit count" %total_match_total_count)
print("%d total truncated text count" %len(total_truncated_list))

ending_time = datetime.now()
print(f"\n{str(ending_time-starting_time)} elapsed")
starting_time = ending_time
print("UniProt match on paper text completed")
