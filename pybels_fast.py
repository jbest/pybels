"""
Paring down the processing by assuming that the input is DwC compatible
Consolidating functions into one file

"""

import csv
import os
import zipfile
import glob
from pathlib import Path
import argparse

import pandas as pd

#BELS
from id_utils import dwc_location_hash, location_match_str, super_simplify
from dwca_terms import locationmatchsanscoordstermlist
from bels_query import get_location_by_hashid, row_as_dict
from dwca_vocab_utils import darwinize_dict
from chardet import UniversalDetector
from dwca_utils import safe_read_csv_row, lower_dict_keys

# specifying the zip file name 
#zip_file = '/Volumes/herbarium-data_v3/TORCH-data_snapshots-2024-05-07/ACU-485_DwC-A.zip' 
#zip_dir = '/Volumes/herbarium-data_v3/TORCH-data_snapshots-2024-05-07/' 
#zip_dir = 'data/TORCH-data_snapshots-2024-05-07/'
#zip_dir = 'data/test/'

#zip_dir = '/mnt/DATA3-4TB/BRIT_git/TORCH_TCN_urls/data/TORCH-data_snapshots-2024-06-01/'
#zip_dir = '/mnt/DATA3-4TB/BRIT_git/TORCH_TCN_urls/data/TORCH-data_snapshots-2024-08-19/'
#zip_dir = '/mnt/DATA3-4TB/BRIT_git/TORCH_TCN_urls/data/TORCH-test_badzip/'
# Test for BRIT UT georef
#zip_dir = '/mnt/DATA3-4TB/BRIT_git/TORCH_georef_regions/TORCH-data_snapshots-2024-11-05/'
#zip_dir = '/mnt/DATA3-4TB/BRIT_git/TORCH_georeferencing/data/TORCH-data_snapshots_TX_OK_2024-12-06/'

# path to save TSV file
#output_path = Path(zip_dir) / 'torch_bels_locs.tsv'
#print('output_path', output_path)

def arg_setup():
    # set up argument parser
    ap = argparse.ArgumentParser()
    input_group = ap.add_mutually_exclusive_group()
    input_group.add_argument("-s", "--single_input", required=False, \
        help="Input directory path for a single CSV file.")
    input_group.add_argument("-z", "--zip_dir", required=False, \
        help="Input directory path for one or more ZIP files in DwCA format.")
    ap.add_argument("-v", "--verbose", action="store_true", \
        help="Detailed output.")
    args = vars(ap.parse_args())
    return args


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

def arg_setup():
    # set up argument parser
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input_path", required=False, \
        help="Input file path - must be a TSV file")
    ap.add_argument("-z", "--zip_dir", required=False, \
        help="Input directory path scanned for ZIP files")
    ap.add_argument("-v", "--verbose", action="store_true", \
        help="Detailed output.")
    args = vars(ap.parse_args())
    return args


if __name__ == '__main__':
    
    def pre_filter(df = None):
        print('Filtering for Texas and Oklahoma occurrences only.')
        return df[(df['stateProvince'] == 'Texas') | (df['stateProvince'] == 'Oklahoma')]

    # set up argparse
    args = arg_setup()
    verbose = args['verbose']
    input_path = args['input_path']
    zip_dir = args['zip_dir']

    if input_path:
        print('Will load:', input_path)
    elif zip_dir:
        print('Will load ZIPs in:', zip_dir)
    else:
        print('ZIP directory path (-z) or TSV file path (-i) needed.')

    if input_path:
        df = pd.read_csv(input_path, sep='\t', low_memory=False, on_bad_lines='skip')
        print('Data shape', df.shape)
        df_torch = df[(df['stateProvince'] == 'Texas') | (df['stateProvince'] == 'Oklahoma')]
        print('Filtered TX OK', df_torch.shape)
        # apply solution
        # Generate loc strings
        print('Generating BELS location strings for:', input_path)
        df_torch['bels_location_string'] = df_torch.apply(bels_simplify, axis=1)
        df_torch.to_csv('BELS_test_XXX.tsv', sep='\t')

    if zip_dir:
        # opening the zip file in READ mode 
        zip_files = glob.glob(zip_dir + '*.zip')
        #print(zip_files)

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
                    df = pd.read_csv(dwca.open('occurrences.csv'), low_memory=False)
                    # filter records not needed - otherwise memory issues
                    print('Before filter:', var_name, df.shape)
                    df_dict[var_name] = pre_filter(df)
                    print('After filter:', var_name, df_dict[var_name].shape)
                    #print(df_dict[var_name].shape)
                elif 'occurrence.txt' in dwca.namelist():
                    # special case for UT PRC which is from IPT
                    df = pd.read_csv(dwca.open('occurrence.txt'), sep='\t', low_memory=False, on_bad_lines='skip')
                    # filter records not needed - otherwise memory issues
                    print('Before filter:', var_name, df.shape)
                    df_dict[var_name] = pre_filter(df)
                    print('After filter:', var_name, df_dict[var_name].shape)            
                    print(df_dict[var_name].shape)
                elif 'occurrences.tab' in dwca.namelist():
                    df = pd.read_csv(dwca.open('occurrences.tab'), sep='\t', low_memory=False, on_bad_lines='skip')
                    # filter records not needed - otherwise memory issues
                    print('Before filter:', var_name, df.shape)
                    df_dict[var_name] = pre_filter(df)
                    print('After filter:', var_name, df_dict[var_name].shape)
            except zipfile.BadZipFile as e:
                print('Unable to read bad zipfile:', zip_file)
                #TODO indicate problem in output
            #TODO raise exception or alert if no matching occ file found

        print('Load of DWCA ZIPs complete.')
        torch_list=[]

        for coll in df_dict:
            df = df_dict[coll]
            print('Coll. shape', coll, df.shape)
            df_torch = df[(df['stateProvince'] == 'Texas') | (df['stateProvince'] == 'Oklahoma')]
            print('Filtered TX OK', coll, df_torch.shape)
            # by iterating
            # apply solution
            # Generate loc strings
            print('Generating BELS location strings for:', coll)
            df_torch['bels_location_string'] = df_torch.apply(bels_simplify, axis=1)
            #TODO - add a hash string based on bels_location_string. Instead of using the dwc_hash, use a similar method but rewrite with a cached property
            # this will provide a hash that will match across any version of the dataset as opposed to the loc_id I'm currently using
            # Save to TSV
            #df_torch.to_csv(coll + '.tsv', sep='\t')
            torch_list.append(df_torch)


        print('Concatenating DWCAs')
        df_all = pd.concat(torch_list)

        #df_all.to_csv('torch_bels_locs.csv', index=False, sep='\t')
        df_all.to_csv(output_path, index=False, sep='\t')

        print('Concatenated, filtered and BELS-enhanced DwCAs saved to TSV:', output_path)
