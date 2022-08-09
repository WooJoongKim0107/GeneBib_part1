# -*- coding:utf-8 -*-

'''
python ver. 3.6.12

Read Research article record obtained from PubMed
and separate those published from selected journals

input : PumMed baseline file in '.xml' format from PubMed FTP
output : '.pickle' text file of papers published in US


'''

import datetime
import os
import csv
import gzip
import xml.etree.ElementTree as ET
from multiprocessing import Pool
import pickle
import numpy as np

starting_time = datetime.datetime.now()

# Directory paths of PubMed basline data files in '.xml' format
# paper_record_dir = '/media/wcjung/HDD_barracuda_8T/wcjung/Work/meMetrics3/data/paperRecord'
pubmed_dir = '/home/data/wcjung/pubmed'
paper_record_dir = f'{pubmed_dir}/paper_220422'
xml_base_pathList = []
for root, d_names, f_names in os.walk(paper_record_dir):
    for f in f_names:
        if f.startswith("pubmed22n") and f.endswith(".xml.gz"):
            xml_base_pathList.append(os.path.join(root,f))

xml_base_pathList.sort()
print("%d PubMed baseline file pathed found" %len(xml_base_pathList))

jrnl_list_dir = "/home/wcjung/Work/meMetrics3/data/paperRecord"


#===============================================================================
# JCR-18-ed mapping 

# Journal information as a key and integer index as a value
journal_title_dict = dict()
journal_abbrv_dict = dict()
journal_issn_dict = dict()

# List of tuple of journal info.; (journal_title, journal_abbrv, journal_issn)
wos_journal_list = []

# InCite list of 3881(3870+11) journals in the exproted format as it is
wos_journal_file = open(f"{jrnl_list_dir}/_total_journal_list_200514.csv"
                        , 'r', newline='')

numset = set(str(i) for i in range(10))
cnt = 0
wos_journal_csv_reader = csv.reader(wos_journal_file, delimiter=',', quotechar='"')
for row_i in wos_journal_csv_reader:
    
    journal_rank = row_i[0]
    if not journal_rank[0] in numset : continue
    
    # The tiltes, abbreviations, and ISSN are converted to lowercases
    journal_title = row_i[1].lower()
    journal_title_dict[journal_title] = cnt
    journal_abbrv = row_i[2].lower()
    journal_abbrv_dict[journal_abbrv] = cnt
    journal_issn = row_i[3].lower()
    journal_issn_dict[journal_issn] = cnt
    
    wos_journal_list.append((journal_title, journal_abbrv, journal_issn))
    cnt += 1
    pass    

print("%6d journals found" %cnt)
wos_journal_file.close()


jo_info_path = f'{jrnl_list_dir}/wos_pubmed_map'
os.makedirs(jo_info_path, exist_ok=True)

outFile = open(jo_info_path+"/total_jo_info_set_200517_02.pyc", 'rb')
total_jo_info_set = pickle.load(outFile)
outFile.close()

print("%6d : total_jo_info_set" %len(total_jo_info_set))

# Add journal integer index for those journals newly retrieved
# by related jouranl information
for map_update_item in total_jo_info_set :
    if map_update_item[0] == 0:
        journal_issn_dict[map_update_item[1]] = map_update_item[2]
    if map_update_item[0] == 1:
        journal_title_dict[map_update_item[1]] = map_update_item[2]
    if map_update_item[0] == 2:
        journal_abbrv_dict[map_update_item[1]] = map_update_item[2]



#===============================================================================
# JCR-22-mjl mapping 

jcr_journal_file = open(paper_record_dir+"/wos-core_SCIE 2022-April-19_selected.csv"
                        , 'r', newline='')
cnt = 0
numset = set(str(n) for n in range(10))

jcr_22_journals = []
wos_journal_csv_reader = csv.reader(jcr_journal_file, delimiter=',', quotechar='"')
headers = [c.strip('"') for c in jcr_journal_file.readline().strip().split(',')]
for row_i in wos_journal_csv_reader:
    journal_rank = row_i[0]
    
    # Skip rows containing irrelavant information in the '.csv' file
    # if not journal_rank[0] in numset: continue
    
    jcr_22_journals.append(tuple(row_i[0:3]+row_i[6:]))
    cnt += 1
    pass    

print("%6d journals found" %cnt) # 4020 + 11
jcr_journal_file.close()


# Load previously existing journal retrieval data if any
with open(f"{jo_info_path}/total_jo_info_dict_220520_jcr22.pkl", 'rb') as i_f:
    jr_titles2idx, jr_issns2idx, jr_abbrev2idx = pickle.load(i_f)

print(f"jr_titles2idx: {len(jr_titles2idx)}")
print(f"jr_issns2idx: {len(jr_issns2idx)}")
print(f"jr_abbrev2idx: {len(jr_abbrev2idx)}")


ending_time = datetime.datetime.now()
print(ending_time-starting_time)
starting_time = datetime.datetime.now()



def get_element_text(element):
    """
    Return the lowercase text content of a xml element if any
    """
    if element.text != None:
        element_text = element.text.lower()
    else:
        element_text = ""
    
    return element_text


def paper_extract_call(path_idx) :
    """
    Call function for reading and pickling research articles of interest
    """
    idx_pair_list= np.ndarray((0,3), dtype=np.int16)
    yr_hist = [0]*2030
    pyper_dict_list = []
    tot_pyper_cnt = 0
    pyper_cnt = 0
    pyper_file_cnt = 0
    
    path_i = xml_base_pathList[path_idx]
    tree_0 = ET.parse(gzip.open(path_i, 'r'))
    root_0 = tree_0.getroot()
    
    # Loop over the articles in the xml root element of baseline file
    for pubm_idx, pubm_i in enumerate(root_0) :
        elem_medline = pubm_i.find("MedlineCitation")
        
        # Read basic informations of each article
        elem_articl = elem_medline.find("Article")
        elem_medl_jo = elem_medline.find("MedlineJournalInfo")
        elem_pmid = elem_medline.find("PMID")
        
        # Read informations for journal identification
        elem_journ = elem_articl.find("Journal")
        elem_jo_abbr = elem_journ.find("ISOAbbreviation")
        elem_jo_issn = elem_journ.find("ISSN")
        elem_jo_ttl = elem_journ.find("Title")        
        elem_medl_issn = elem_medl_jo.find("ISSNLinking")
        elem_medl_jo_ttl = elem_medl_jo.find("MedlineTA")
        
        # Skip the item in the loop if the language is not the English
        elem_lang = elem_articl.find("Language")
        if elem_lang != None and elem_lang.text != 'eng' : continue
        
        # Bring the integer index of the journal as it is from WoS InCite list
        wos_jo_idx = -1
        if elem_jo_issn != None:
            jo_issn = get_element_text(elem_jo_issn)
            if jo_issn in journal_issn_dict : 
                wos_jo_idx = journal_issn_dict[jo_issn]
        elif elem_medl_issn != None:
            medl_issn = get_element_text(elem_medl_issn)
            if medl_issn in journal_issn_dict : 
                wos_jo_idx = journal_issn_dict[medl_issn]
        elif elem_jo_ttl != None:
            jo_ttl = get_element_text(elem_jo_ttl)
            if jo_ttl in journal_title_dict : 
                wos_jo_idx = journal_title_dict[jo_ttl]
        elif elem_jo_abbr != None:
            jo_abbr = get_element_text(elem_jo_abbr)
            if jo_abbr in journal_abbrv_dict : 
                wos_jo_idx = journal_abbrv_dict[jo_abbr]
        elif elem_medl_jo_ttl != None:
            medl_jottl = get_element_text(elem_medl_jo_ttl)
            if medl_jottl in journal_abbrv_dict : 
                wos_jo_idx = journal_abbrv_dict[medl_jottl]
        
        # Skip the item in the loop if no journal is retrieved
        if wos_jo_idx < 0: #JCR-22 mapping
            for issn in (elem_jo_issn, elem_medl_issn):
                if issn in jr_issns2idx:
                    wos_jo_idx = jr_issns2idx[issn]+10000
                    break
            
        if wos_jo_idx < 0: #JCR-22 mapping
            for ttl in (elem_jo_ttl, ):
                if ttl in jr_titles2idx:
                    wos_jo_idx = jr_titles2idx[ttl]+10000
                    break
            
        if wos_jo_idx < 0: continue
        
        # Write the location of articels at the level of the basline files
        idx_pair = np.array((path_idx, pubm_idx, wos_jo_idx), dtype=np.int16)
        idx_pair_list = np.r_[idx_pair_list,[idx_pair]]
        
        # initialize a data type for  the pubmed article as a dintinary
        pyper_dict = {"WOS" : wos_jo_idx, "PMID" : elem_pmid}
        
        titl_text = ""
        abst_text = ""
        date_text = ""
        
        # Retrieve the title, abstract, and date of publication
        elem_title = elem_articl.find("ArticleTitle")
        if elem_title!=None and elem_title.text!=None:
            titl_text = elem_title.text+"\n"
            pyper_dict.setdefault('TI', []).append(titl_text)
        
        elem_abstr = elem_articl.find("Abstract")
        if elem_abstr!=None and elem_abstr.text!=None:
            elem_abs_txt_list = elem_abstr.findall("AbstractText")
            if elem_abs_txt_list != None :
                for elem_abs_txt in elem_abs_txt_list :
                    if (abst_text:=elem_abs_txt.text) != None :
                        pyper_dict.setdefault('AB', []).append(abst_text)
        
        elem_pubdate = elem_journ.find("JournalIssue").find("PubDate")
        if elem_pubdate != None and elem_pubdate.text != None :
            elem_year = elem_pubdate.find("Year")
            elem_medldate = elem_pubdate.find("MedlineDate")
            
            if elem_year != None and elem_year.text != None :
              date_text += elem_year.text
              pub_yr = int(elem_year.text)
              elem_month = elem_pubdate.find("Month")
              if elem_month != None and elem_month.text != None :
                  date_text += ' '+elem_month.text
                  elem_day = elem_pubdate.find("Day")
                  if elem_day != None and elem_day.text != None :
                      date_text += ' '+elem_day.text
              pyper_dict.setdefault('DP', []).append(date_text)
            
            elif elem_medldate != None and elem_medldate.text != None :
              date_text += elem_medldate.text
              pyper_dict.setdefault('DP', []).append(date_text)
              pubDateSplit = date_text.split(' ')
              if set(pubDateSplit[0]) <= numset :
                  pub_yr = int(pubDateSplit[0])
              elif '-' in set(pubDateSplit[0]):
                  pub_yr = int(pubDateSplit[0].split('-')[1])
              elif set(pubDateSplit[1]) <= numset :
                  pub_yr = int(pubDateSplit[1])
              else :
                  pub_yr = 0
            
            yr_hist[pub_yr] += 1
        
        pyper_cnt += 1
        
        # Save every 10,000 PubMed articles into a pickled file
        pyper_dict_list.append(pyper_dict)
        if pyper_cnt == 10000:
            outFile = open(pyper_file_path+"/ppr_%04d_%02d.pkl"%(path_idx, pyper_file_cnt), 'wb')
            pickle.dump(pyper_dict_list, outFile)
            outFile.close()
            pyper_dict_list.clear()
            pyper_file_cnt += 1
            tot_pyper_cnt += pyper_cnt
            pyper_cnt = 0
    
    # Save last pickled file for the xml file
    if pyper_cnt != 0:
        outFile = open(pyper_file_path+"/ppr_%04d_%02d.pkl"%(path_idx, pyper_file_cnt), 'wb')
        pickle.dump(pyper_dict_list, outFile)
        outFile.close()
        pyper_dict_list.clear()
        tot_pyper_cnt += pyper_cnt
    
    # Write the numbers of articles counted for each year
    yr_count_list = [yc for yc in enumerate(yr_hist) if yc[1]>0]
    
    print("xml file No. %4d -  %d papers extracted"%(path_idx, tot_pyper_cnt))
    
    return idx_pair_list, yr_count_list


pyper_file_path = f'{pubmed_dir}/pickled_pubmed'
os.makedirs(pyper_file_path, exist_ok=True)

# Map the call function to multiple workers
total_idx_pair_list = np.ndarray((0,3), dtype=np.int16)
total_bio_paper_hist = [0]*2030

pooling_len = len(xml_base_pathList)

with Pool(4) as pubmed_extract_pool :
    for return_tuple in pubmed_extract_pool.imap(paper_extract_call, range(pooling_len)) :
        total_idx_pair_list = np.r_[total_idx_pair_list, return_tuple[0]]
        for yr, cnt in return_tuple[1]:
            total_bio_paper_hist[yr] += cnt

print(len(total_idx_pair_list))
print(sum(total_bio_paper_hist))

outFile = open(pyper_file_path+"/hit_pyp_loc_220517.pkl", 'wb')
pickle.dump(total_idx_pair_list, outFile)
outFile.close()


ending_time = datetime.datetime.now()
print(ending_time-starting_time)
starting_time = datetime.datetime.now()

