# -*- coding:utf-8 -*-

'''
python ver. 3.6.12

Read xml base file of UniProt.

input : 'uniprot_sprot.xml.gz' from UniProt ftp site
output : pickle UniProt data as listed xml tree element


'''


import os
import datetime
import pickle
import xml.etree.ElementTree as ET


# input_dir = '/home/data/wcjung/Work/geneBib'
input_dir = '/home/data/wcjung/uniprot'
# schema_file = '/uniprot.xsd'

# open compressed file
# input_file = '/uniprot_sprot.xml.gz'
# uniprot_file = gzip.open(input_dir+input_file, 'rt')
# elapsed time : 0:02:14.817257 reading 10,000 entries

# input_file = '/uniprot_sprot_2019_01.xml'
# input_file = '/uniprot_sprot.xml.gz'
input_file = '/uniprot_sprot.xml'
# elapsed time : 0:01:39.906210 reading 10,000 entries

# output_dir = '/home/data/wcjung/Work/geneBib/pyrotin_2019_01'
output_dir = '/home/data/wcjung/Work/geneBib/pyrotin_2022_05'
os.makedirs(output_dir, exist_ok=True)

def printElement(xmlTreeElem) :
    '''
    print tag and child elements of a xml element.
    needs a etree.ElementTree object from xml package as input argument.
    '''
    print("tag : %s" %xmlTreeElem.tag)
    print("attrib : %s" %xmlTreeElem.attrib)
    print("text : %s" %xmlTreeElem.text)
    if len(xmlTreeElem) > 0:
        for child in xmlTreeElem :
            print("child : %s" %child.tag)
            if child.tag == 'classification-symbol' :
                print("symbol : %s" %child.text)
    print()




if __name__ == '__main__':
    starting_time = datetime.datetime.now()
    
    # uniprot_file = gzip.open(input_dir+input_file, 'r')
    uniprot_file = open(input_dir+input_file, 'r')
    uniprot_file.seek(0)
    uniprot_parser = ET.XMLPullParser()
    
    # Count the number of UniProt entries read
    entry_counter = 0
    # Count the number of file containing xml elements of entries
    outfile_counter = 0
    
    element_test_list = []
    
    file_pos = -1
    while file_pos != uniprot_file.tell():  
        file_pos = uniprot_file.tell()
        uniprot_parser.feed(uniprot_file.readline())
        
        for event, element in uniprot_parser.read_events():
            
            if not element: continue
            if not element.tag == '{http://uniprot.org/uniprot}entry': continue
            if not event == 'end': continue
            
            entry_counter += 1
            element_test_list.append(element)
            # Reset the xml parser module,emptying memory of already read portion of input file
            uniprot_parser = ET.XMLPullParser()
            
            # Save xml element list of every 100 UniPort entries as pickled file.
            if len(element_test_list) == 100:
                with open(output_dir+f"/pyrotin_{outfile_counter:04d}.pickle", 'wb') as outfile:
                    pickle.dump(element_test_list, outfile)
                print(f"outfile_counter : {outfile_counter}")
                outfile_counter += 1
                element_test_list = []
    
    if len(element_test_list) > 0:
        with open(output_dir+f"/pyrotin_{outfile_counter:04d}.pickle", 'wb') as outfile:
            pickle.dump(element_test_list, outfile)
        outfile_counter += 1
        
    print(f"outfile_counter : {outfile_counter}")
    print(f"entry_counter : {entry_counter}")
    
    ending_time = datetime.datetime.now()
    print(f"elapsed time : {ending_time-starting_time}")
    starting_time = ending_time
    

