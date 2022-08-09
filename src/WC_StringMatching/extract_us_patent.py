# -*- coding:utf-8 -*-

'''

Read patent raw data obtained from Google Cluod Platform and separate US patents into '.json' files

input : Patent data in '.json.gz' format from Google Cloud Platform
output : '.json' text file of patents published in US


'''

import gzip
import json
import datetime
import os
from multiprocessing import Pool

starting_time = datetime.datetime.now()

# loaction of json.gz files downloaded from Cloud Storage as input
# patent_input_path = './full_raw_200920'
patent_input_path = '/home/data/wcjung/patent/full_raw_220429'

# loaction of json files as output
# output_path = './us_raw_200920'
output_path = '/home/data/wcjung/patent/us_raw_220429'

if __name__ == "__main__":
    
    os.makedirs(output_path, exist_ok=True)

    patent_paths = []
    for root, d_names, f_names in os.walk(patent_input_path):
        for f in f_names:
            if f.startswith("patent"):
                patent_paths.append(os.path.join(root,f))

    patent_paths.sort()
    print("%d patent file pathes found."%len(patent_paths))


    def patent_extract_call(patent_path_idx) :
        '''
        Call function for multiprocessing of US patent extraction
        '''
        
        patent_path = patent_paths[patent_path_idx]
        
        # Check broken gzip file; better use checksum files instead
        try:
            patent_file = gzip.open(patent_path, 'rt')
            first_line = patent_file.readline()
        except gzip.BadGzipFile:
            patent_file.close()
            patent_file = open(patent_path, 'r')

        patent_file.seek(0)
        
        # Open file stream for output file
        out_file_path = output_path+"/us_patent_%04d.json"%patent_path_idx
        out_file = gzip.open(out_file_path, 'wt') # open file with text-writing mode
        # out_file = open(out_file_path, 'wt') # open file with text-writing mode
        
        us_patent_count = 0
        for patent_idx, entry_line in enumerate(patent_file) :
            
            # Check broken '.json' file
            try :
                patent_entry = json.loads(entry_line)
            except json.decoder.JSONDecodeError:
                continue
            
            if patent_entry["country_code"] != 'US' : continue
            
            us_patent_count += 1
            written_byte = out_file.write(entry_line)
        
        out_file.close()
        patent_file.close()
        
        print('file No. %4d finished' %patent_path_idx)
        ending_time = datetime.datetime.now()
        
        return us_patent_count


    ending_time = datetime.datetime.now()
    starting_time = ending_time

    # Call the functions into multiprocessor module for patent extraction
    processor_num = 6

    processing_list_size = len(patent_paths[:])
    begin_idx = 0
    total_extracted_count = 0
    with Pool(processor_num) as patent_extract_workers :
        proc_result_iter = patent_extract_workers.imap_unordered(
                    patent_extract_call, range(begin_idx, processing_list_size) )
        for result_array in proc_result_iter :
            total_extracted_count += result_array

    print("%d total US patents" %total_extracted_count)

    ending_time = datetime.datetime.now()
    print(str(ending_time - starting_time)+" elapsed")
    starting_time = ending_time

    print("US patnet extraction completed")
