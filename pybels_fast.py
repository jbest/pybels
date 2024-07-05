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

def bels_simplify(occurrence):

    location = dict(
        ID=occurrence.get('id'),
        continent=occurrence.get('continent'),
        country=occurrence.get('country'),
        countrycode=occurrence.get('countrycode'),
        stateprovince=occurrence.get('stateprovince'),
        county=occurrence.get('county'),
        locality=occurrence.get('locality')
    )
    #print(occurrence)

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


# specifying the zip file name 
#zip_file = '/Volumes/herbarium-data_v3/TORCH-data_snapshots-2024-05-07/ACU-485_DwC-A.zip' 
#zip_dir = '/Volumes/herbarium-data_v3/TORCH-data_snapshots-2024-05-07/' 
#zip_dir = 'data/TORCH-data_snapshots-2024-05-07/'
#zip_dir = 'data/test/'

zip_dir = '/mnt/DATA3-4TB/BRIT_git/TORCH_TCN_urls/data/TORCH-data_snapshots-2024-06-01/'
#zip_dir = '/mnt/DATA3-4TB/BRIT_git/TORCH_TCN_urls/data/TORCH-test_badzip/'


# opening the zip file in READ mode 
zip_files = glob.glob(zip_dir + '*.zip')
print(zip_files)

df_dict = {}
for zip_file in zip_files:
    var_name = Path(zip_file).stem.replace('-','_')
    var_name = var_name.replace('.','_')
    print('Opening:', zip_file)
    try:
        dwca = zipfile.ZipFile(zip_file, 'r')
        #dwca.printdir()
        print('Reading:', zip_file, 'into', var_name )
        if 'occurrences.csv' in dwca.namelist():
            df_dict[var_name] = pd.read_csv(dwca.open('occurrences.csv'), low_memory=False)
            print(df_dict[var_name].shape)
        elif 'occurrence.txt' in dwca.namelist():
            # special case for UT PRC which is from IPT
            df_dict[var_name] = pd.read_csv(dwca.open('occurrence.txt'), sep='\t', low_memory=False, on_bad_lines='skip')
            print(df_dict[var_name].shape)
        elif 'occurrences.tab' in dwca.namelist():
            df_dict[var_name] = pd.read_csv(dwca.open('occurrences.tab'), sep='\t', low_memory=False, on_bad_lines='skip')
            print(df_dict[var_name].shape)
    except zipfile.BadZipFile as e:
        print('Unable to read bad zipfile:', zip_file)
        #TODO indicate problem in output
    #TODO raise exception or alert if no matching occ file found



print('Load of DWCAs complete.')
torch_list=[]

for coll in df_dict:
    df = df_dict[coll]
    print('Coll. shape', coll, df.shape)
    df_torch = df[(df['stateProvince'] == 'Texas') | (df['stateProvince'] == 'Oklahoma')]
    print('Filtered TX OK', coll, df_torch.shape)
    # by iterating
    """
    for index, row in df_torch.iterrows():
        #print(row['stateProvince'])
        bels_location_string = bels_simplify(occurrence=row)
        if bels_location_string:
            #print(bels_location_string)
            df_torch.loc[index,'bels_location_string'] = bels_location_string
        #df_dict[coll][index]['bels_location_string'] = bels_location_string
        #df_dict[coll][index]['bels_location_string'] = bels_location_string
    """
    # apply solution
    # Generate loc strings
    print('Generating BELS location strings for:', coll)
    df_torch['bels_location_string'] = df_torch.apply(bels_simplify, axis=1)
    # Save to CSV
    df_torch.to_csv(coll + '.csv', sep='\t')
    torch_list.append(df_torch)


print('Concatenating DWCAs')
df_all = pd.concat(torch_list)

df_all.to_csv('torch_bels_locs.csv', index=False, sep='\t')
print('Concatenated DWCAs saved to CSV')
