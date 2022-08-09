# -*- coding:utf-8 -*-

'''
python ver. 3.6.12

Read xml base file of UniProt.

input : pickle UniProt data as listed xml tree element
output : pickled protein/gene synonym list


'''


import os
import datetime
import pickle


input_dir = '/home/data/wcjung/Work/geneBib/pyrotin_2022_05'
# input_dir = '/home/data/wcjung/Work/geneBib/pyrotin_2019_01'
# schema_file = '/uniprot.xsd'
XML_ROOT = "{http://uniprot.org/uniprot}"

# Possible combinations of tag (and attribute) from element tree of a synonym 
level_tag_list = [
 ('protein', 'recommendedName', 'fullName'),
 ('protein', 'recommendedName', 'shortName'),
 ('protein', 'recommendedName', 'ecNumber'),
 ('protein', 'alternativeName', 'fullName'),
 ('protein', 'alternativeName', 'shortName'),
 ('protein', 'alternativeName', 'ecNumber'),
 ('protein', 'allergenName'),
 ('protein', 'cdAntigenName'),
 ('protein', 'innName'),
 
 ('protein', 'component', 'recommendedName', 'fullName'),
 ('protein', 'component', 'recommendedName', 'shortName'),
 ('protein', 'component', 'recommendedName', 'ecNumber'),
 ('protein', 'component', 'alternativeName', 'fullName'),
 ('protein', 'component', 'alternativeName', 'shortName'),
 ('protein', 'component', 'alternativeName', 'ecNumber'),
 ('protein', 'component', 'allergenName'),
 
 ('protein', 'domain', 'recommendedName', 'fullName'),
 ('protein', 'domain', 'recommendedName', 'shortName'),
 ('protein', 'domain', 'recommendedName', 'ecNumber'),
 ('protein', 'domain', 'alternativeName', 'fullName'),
 ('protein', 'domain', 'alternativeName', 'shortName'),
 ('protein', 'domain', 'alternativeName', 'ecNumber'),

 ('gene', 'name', 'primary'),
 ('gene', 'name', 'synonym'),
 ('gene', 'name', 'ordered locus'),
 ('gene', 'name', 'ORF'),
]


def recursive_check(
        element, ascending_nodes, ascending_check_set, attrib_check_set):
    '''
    check tags of child elements and attribute keys of subelements
    Tags sequenced from the root to the leaf element are stored as tuple. The
    leaf nodes contains text contents of its synonym as a gne or protein.
    '''
    # if a.tag == "{http://uniprot.org/uniprot}ecNumber": pass
    short_tag = element.tag[28:]
    ascending_nodes_c = ascending_nodes.copy()
    ascending_nodes_c.append(short_tag)
    if "type" in element.attrib:
        ascending_nodes_c.append(element.attrib["type"])
    
    if element.text != None:
        elem_text = element.text.strip()
        if elem_text != "":
            ascending_check_set.add(tuple(ascending_nodes_c))
    
    for key_i in element.attrib.keys():
        attrib_check_set.add((short_tag, key_i, ))
    
    for child_elem in element:
        recursive_check(
          child_elem, ascending_nodes_c, ascending_check_set, attrib_check_set)


def recursive_extract(entry_elem, ascending_nodes, out_list, tag_dict):
    '''
    '''
    element = entry_elem[0]
    entry_idx = entry_elem[1]
    short_tag = element.tag[28:]
    ascending_nodes_c = ascending_nodes.copy()
    ascending_nodes_c.append(short_tag)
    if "type" in element.attrib:
        ascending_nodes_c.append(element.attrib["type"])
    
    elem_text = element.text.strip()
    if element.tag == f"{XML_ROOT}ecNumber":
        pass
    elif elem_text != "":
        out_list.append((elem_text, 
                          tag_dict[tuple(ascending_nodes_c)], entry_idx))
    
    for child_elem in element:
        recursive_extract((child_elem, entry_idx),
                          ascending_nodes_c, out_list, tag_dict)


def keyword_extract_call(pyrotein_path_idx):
    '''
    Call function for multiprocessing of keyword-tag pair extraction
    '''
    partial_keyword_list = []
    pyrotein_path = pyrotein_path_list[pyrotein_path_idx]
    with open(pyrotein_path, 'rb') as pyrotein_pckl:
      pyrotein_elem_list = pickle.load(pyrotein_pckl)
    
    for pyrotein_idx, pyrotein_elem in enumerate(pyrotein_elem_list):
      entry_idx = (pyrotein_path_idx*100)+pyrotein_idx
      for prot_elem in pyrotein_elem.iter(f'{XML_ROOT}protein'):
        recursive_extract((prot_elem, entry_idx), [],
                            partial_keyword_list, level_tag_dict)
      for gene_elem in pyrotein_elem.iter(f'{XML_ROOT}gene'):
        recursive_extract((gene_elem, entry_idx), [],
                            partial_keyword_list, level_tag_dict)
    
    print(f"{pyrotein_path_idx} file with {len(partial_keyword_list)} keyw")
    return partial_keyword_list



if __name__ == "__main__":
    starting_time = datetime.datetime.now()
    
    pyrotein_path_list = []
    for root, d_names, f_names in os.walk(input_dir):
        for f in f_names:
            if f.startswith("pyrotin"):
                pyrotein_path_list.append(os.path.join(root,f))
    
    pyrotein_path_list.sort()
    print("%d pyrotein files found." %len(pyrotein_path_list))
    
    
    level_tag_dict = dict()
    for tag_id, level_tag in enumerate(level_tag_list):
        level_tag_dict[level_tag] = tag_id
        print(f"{tag_id} : {level_tag}")
    
    starting_time = datetime.datetime.now()
    
    from multiprocessing import Pool
    
    tagged_keyword_list = []
    processor_num = 6
    proc_list_size = len(pyrotein_path_list[:])
    chunk_Size = int(proc_list_size/processor_num)+1
    begin_idx = 0
    with Pool(processor_num) as keyw_extract_workers :
        ovLookResultIter = keyw_extract_workers.imap_unordered(
                                    keyword_extract_call,
                                    range(begin_idx, proc_list_size) )
        for resultArray in ovLookResultIter :
            tagged_keyword_list += resultArray
    
    print(f"tagged_keyword_list : {len(tagged_keyword_list)}")
    tagged_keyword_set = set(tagged_keyword_list)
    print(f"tagged_keyword_set : {len(tagged_keyword_set)}")
    # 929474 2019_01 / 956971 2022_05
    
    tagged_keyword_list.sort(key=lambda i: i[2])
    
    keyw_tag_list = []
    keyw_tag_idx_dict = dict()
    ent_keyw_dict = dict()
    
    for i in tagged_keyword_list:
        keyw_tag = tuple(i[:2])
        entry_idx = i[2]
        
        if not keyw_tag in keyw_tag_idx_dict:
            keyw_tag_idx_dict[keyw_tag] = len(keyw_tag_list)
            keyw_tag_list.append(keyw_tag)
        
        ent_keyw_dict.setdefault(entry_idx, []).append(keyw_tag_idx_dict[keyw_tag]) 
    
    print(f"ent_keyw_dict : {len(ent_keyw_dict)}")
    print(f"keyw_tag_idx_dict : {len(keyw_tag_idx_dict)}")
    
    
    path = "/home/wcjung/Work/geneBib/data/uniprot_xml_parse"
    with open(f"{path}/keyword_list_2022_05.pkl", 'wb') as o_f:
    # with open(f"{path}/keyword_list_2019_01.pkl", 'wb') as o_f:
        pickle.dump(keyw_tag_list, o_f)
    
    with open(f"{path}/entr_to_keyw_2022_05.pkl", 'wb') as o_f:
    # with open(f"{path}/entr_to_keyw_2019_01.pkl", 'wb') as o_f:
        pickle.dump(ent_keyw_dict, o_f)
    
    
    ending_time = datetime.datetime.now()
    print(f"elapsed time : {ending_time-starting_time}")
    starting_time = ending_time
    

