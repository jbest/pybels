"""
Paring down the processing by assuming that the input is DwC compatible
Consolidating functions into one file

"""


import csv
import os
import zipfile
import glob
from pathlib import Path

import pandas as pd

#BELS
from id_utils import dwc_location_hash, location_match_str, super_simplify
from dwca_terms import locationmatchsanscoordstermlist
from bels_query import get_location_by_hashid, row_as_dict
from dwca_vocab_utils import darwinize_dict
from chardet import UniversalDetector
from dwca_utils import safe_read_csv_row, lower_dict_keys

def bels_simplify(occurrence=None):

    location = dict(
        ID=occurrence.get('id'),
        continent=occurrence.get('continent'),
        country=occurrence.get('country'),
        countrycode=occurrence.get('countrycode'),
        stateprovince=occurrence.get('stateprovince'),
        county=occurrence.get('county'),
        locality=occurrence.get('locality')
    )

    dirname = os.path.dirname(__file__)
    vocabpath = os.path.join(dirname, './vocabularies/')
    darwincloudfile = os.path.join(vocabpath, 'darwin_cloud.txt')
    row_dict = {}
    for item in list(occurrence.items()):
        row_dict[item[0]]=item[1]

    """
    #loc = darwinize_dict(row_as_dict(occurrence), darwincloudfile)
    """
    #lowerloc = lower_dict_keys(loc)
    #just assume no changes are needed to keys
    lowerloc = lower_dict_keys(row_dict)
    sanscoordslocmatchstr = location_match_str(locationmatchsanscoordstermlist, lowerloc)
    matchstr = super_simplify(sanscoordslocmatchstr)
    return matchstr

# try zip file

# importing required modules 


# specifying the zip file name 
#zip_file = '/Volumes/herbarium-data_v3/TORCH-data_snapshots-2024-05-07/ACU-485_DwC-A.zip' 
zip_dir = '/Volumes/herbarium-data_v3/TORCH-data_snapshots-2024-05-07/' 

# opening the zip file in READ mode 


#zip_files = glob.glob('*.zip')
zip_files = glob.glob(zip_dir + '*.zip')
print(zip_files)

df_list = []
df_dict = {}
for zip_file in zip_files:
    var_name = Path(zip_file).stem.replace('-','_')
    var_name = var_name.replace('.','_')
    dwca = zipfile.ZipFile(zip_file, 'r')
    dwca.printdir()
    print('Reading:', zip_file, 'into', var_name )
    if 'occurrences.csv' in dwca.namelist():
        df_dict[var_name] = pd.read_csv(dwca.open('occurrences.csv'), low_memory=False)
    elif 'occurrence.txt' in dwca.namelist():
        # special case for UT PRC which is from IPT
        df_dict[var_name] = pd.read_csv(dwca.open('occurrence.txt'), sep='\t', low_memory=False, on_bad_lines='skip')
    elif 'occurrences.tab' in dwca.namelist():
        df_dict[var_name] = pd.read_csv(dwca.open('occurrences.tab'), sep='\t', low_memory=False, on_bad_lines='skip')
        #print(df_dict[var_name].shape)
        #dwca-prc-torch-v1.130.zip
        #dwca_ut = zipfile.ZipFile('dwca-prc-torch-v1.130.zip', 'r')
        #df_ut = pd.read_csv(dwca_ut.open('occurrence.txt'), sep='\t', low_memory=False, on_bad_lines='skip')
    if df_dict[var_name]:
        print(df_dict[var_name].shape)

#print(df_dict)
print('Load of DWCAs complete.')

"""

with open('bels_sample_input.csv', newline='', encoding="utf-8-sig") as inputfile:
    reader = csv.DictReader(inputfile)
    for row in reader:
        print('Searching for dups for id:', row['id'])
        response = bels_simplify(occurrence=row)
        print(response)
"""


