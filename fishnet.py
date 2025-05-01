"""
Based on the filtering processs in CountyChopper. 
Meant to be used after pybels_fast.py
and before BELS_Grouper_simple.py

"""


import os
import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from tqdm import tqdm
import re
import argparse
from pathlib import Path

def arg_setup():
    # set up argument parser
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=True, \
        help="Input directory path for a single CSV file.")
    ap.add_argument("-o", "--out", required=False, \
        help="Output directory path for multiple CSV files.")
    ap.add_argument("-v", "--verbose", action="store_true", \
        help="Detailed output.")
    args = vars(ap.parse_args())
    return args

# Function to normalize county names
def normalize_county_name(county_name):
    if isinstance(county_name, str):
        county_name = county_name.lower().strip()
        county_name = re.sub(r'(county|co\.?|[\?\.])$', '', county_name).strip()
        return county_name.title()
    else:
        return ''

# Function to count null collectionCodes and suggest substitution
def count_null_collectioncode(df):
    missing_collectioncode = df['collectionCode'].isna() | (df['collectionCode'] == '')
    null_count = missing_collectioncode.sum()

    if null_count > 0:
        substitute_counts = df.loc[missing_collectioncode, 'institutionCode'].value_counts()
        print(f"\nNumber of records with null or empty 'collectionCode': {null_count}")
        print("Potential substitutions using 'institutionCode':")
        print(substitute_counts)

        # Ask user if they want to make the substitution
        choice = input("\nWould you like to substitute missing 'collectionCode' values with 'institutionCode'? (yes/no): ").strip().lower()
        if choice == 'yes':
            df.loc[missing_collectioncode, 'collectionCode'] = df.loc[missing_collectioncode, 'institutionCode']
            print("Substitution applied.\n")
        else:
            print("No changes made.\n")

    return df

# Function to read configurations from Chopper_Config.txt in the script folder
def load_configurations(config_path=None):
    #script_dir = os.path.dirname(os.path.realpath(__file__))
    #config_file = os.path.join(script_dir, 'Chopper_Config.txt')

    if not os.path.exists(config_path):
        print("Configuration file 'Chopper_Config.txt' not found in script directory.")
        return None

    collection_whitelist = set()
    collection_blacklist = set()
    counties = set()
    state_name = None
    in_county_section = False

    with open(config_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line.startswith("collectionWhitelist:"):
                collection_whitelist.update(map(str.strip, line.split(":", 1)[1].split(',')))
            elif line.startswith("collectionBlacklist:"):
                collection_blacklist.update(map(str.strip, line.split(":", 1)[1].split(',')))
            elif line.startswith("state:"):
                state_name = line.split(":", 1)[1].strip()
            elif line.startswith("counties:"):
                in_county_section = True
            elif in_county_section and line:
                counties.add(normalize_county_name(line))

    return collection_whitelist, collection_blacklist, counties, state_name

def sanitize_filename(filename, replacement=''):
    """
    Sanitizes a string to be safe for use as a filename.

    Args:
        filename (str): The filename to sanitize.
        replacement (str, optional): The string to replace invalid characters with. Defaults to ''.

    Returns:
        str: The sanitized filename.
    """
    # Remove or replace invalid characters
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1F]', replacement, filename)
    # Remove leading and trailing spaces
    filename = filename.strip()
    # Shorten filename if it exceeds 255 characters
    filename = filename[:255]
    return filename

def filter_data(data=None, collection_whitelist=None, collection_blacklist=None, 
    coordinate_uncertainty_threshold=None, filter_coordinate_uncertainty_null=None,
    filterGeoreferenceRemarks=None, filterGeoreferencedBy=None):
    if 'stateProvince' not in data.columns or 'county' not in data.columns or 'collectionCode' not in data.columns:
        # This error was raised when a TSV was opened as a CSV
        print('columns:', data.columns)
        raise ValueError("The CSV file must contain 'stateProvince', 'county', and 'collectionCode' columns.")


    print('Pre-filter data shape:', data.shape)

    # Run the pre-check for missing collectionCode
    #data = count_null_collectioncode(data)

    #pybels normalizes county name, not needed here
    #data['normalized_county'] = data['county'].apply(normalize_county_name)

    # Apply whitelist/blacklist filter for collectionCode
    # TODO not sure what this is used for. 
    #data['collectionCode'] = data['collectionCode'].astype(str)
    #print('Pre-filter data shape:', data.shape)
    
    # Separate out records that are whitelisted
    whitelist_data = data[data['institutionCode'].isin(collection_whitelist)]
    print('whitelist_data shape:', whitelist_data.shape)

    # Exclude records with institutionCode on the blacklist
    # Changing this to institutionCode, originally collectionCode
    data = data[~data['institutionCode'].isin(collection_blacklist)]
    print('Blacklist removed (whitelist + graylist) data shape:', data.shape)

    # Create graylist, will add whitelist back after further filtering
    graylist_data = data[~data['institutionCode'].isin(collection_whitelist)]
    print('Graylist (no whitelist, no blacklist) data shape:', graylist_data.shape)
    # Remove records with null locality
    graylist_data = graylist_data[graylist_data['locality'].notnull()]
    print('Graylist with locality data shape:', graylist_data.shape)
    # Apply coordinateUncertaintyInMeters filter
    # TODO TEMP
    #coordinate_uncertainty_threshold = 10000
    #filter_coordinate_uncertainty_null = True
    if filter_coordinate_uncertainty_null:
        graylist_data = graylist_data[(graylist_data['coordinateUncertaintyInMeters'] < coordinate_uncertainty_threshold)]
        print(f'Graylist with coordinateUncertaintyInMeters < {coordinate_uncertainty_threshold} data shape:', graylist_data.shape)
    # Apply filter to exclude records with null CoordinateUncertainty if enabled
    if filter_coordinate_uncertainty_null:
        graylist_data = graylist_data[graylist_data['coordinateUncertaintyInMeters'].notnull()]
    
    # Apply georeferencedBY and georeferenceRemarks filters if toggled on in the config
    if filterGeoreferencedBy:
        graylist_data = graylist_data[graylist_data['georeferencedBy'].notnull()]
        print('Graylist with null georeferencedBy data shape:', graylist_data.shape)

    if filterGeoreferenceRemarks:
        graylist_data = graylist_data[graylist_data['georeferenceRemarks'].notnull()]
        print('Graylist with null georeferenceRemarks data shape:', graylist_data.shape)

    # Graylist summary before merging
    graylist_institutions = graylist_data['institutionCode'].unique()
    print('Graylist institution codes:', graylist_institutions)

    # Combine the filtered data with the whitelisted data that bypasses all filters
    final_data = pd.concat([whitelist_data, graylist_data])
    print('final_data (whitelist_data concat graylist_data) shape', final_data.shape)

    return final_data

# moving config load to main
if __name__ == "__main__":
    args = arg_setup()

    if args['input']:
        input_csv = Path(args['input']).resolve()

    print('input_csv:', input_csv)

    # Load configurations from Chopper_Config.txt
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_file_path = os.path.join(script_dir, 'Chopper_Config.txt')
    
    config = load_configurations(config_path=config_file_path)
    if not config:
        print('config not loaded')
    else:
        #TODO load from config:
        coordinate_uncertainty_threshold=10000
        filter_coordinate_uncertainty_null=True
        filterGeoreferencedBy = True
        filterGeoreferenceRemarks = True
        collection_whitelist, collection_blacklist, county_list, state_name = config
        print(f'Retaining all records from whitelist: {collection_whitelist}')
        print(f'Excluding all records from blacklist: {collection_blacklist}')
        print('Filtering graylist:')
        print(f'Excluding all > coordinate_uncertainty_threshold: {coordinate_uncertainty_threshold}')
        print(f'Excluding all records filter_coordinate_uncertainty_null: {filter_coordinate_uncertainty_null}')
        print(f'Excluding all null values of filterGeoreferencedBy: {filterGeoreferencedBy}')
        print(f'Excluding all null values of filterGeoreferenceRemarks: {filterGeoreferenceRemarks}')

        if args['out']:
            print('output path:', args['out'])
            output_dir = Path(args['out']).resolve()
        else:
            output_dir = "fishnet_output"
        print('output_dir:', output_dir)

        print('Loading data...')
        try:
            #data = pd.read_csv(input_csv, encoding='ISO-8859-1', dtype=str, on_bad_lines='skip', low_memory=False)
            # Loading TSV format
            df_data = pd.read_csv(input_csv, on_bad_lines='skip', low_memory=False, sep='\t')
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            print(f"Error reading the file: {e}")
            df_data = None

        print('Input data shape', df_data.shape)
        df_filtered = filter_data(data=df_data, collection_whitelist=collection_whitelist, 
            collection_blacklist=collection_blacklist, 
            filter_coordinate_uncertainty_null=filter_coordinate_uncertainty_null,
            coordinate_uncertainty_threshold=coordinate_uncertainty_threshold,
            filterGeoreferencedBy = filterGeoreferencedBy,
            filterGeoreferenceRemarks = filterGeoreferenceRemarks
        )
        print('Filtered data shape', df_filtered.shape)
        # drop any unnamed columns
        df_filtered.drop(df_filtered.columns[df_filtered.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
        print('Filtered data shape (unnamed removed)', df_filtered.shape)

        if input_csv:
            # Create output filename
            input_filename_stem = input_csv.stem
            output_filename = input_filename_stem + '_filtered.csv'
            #print('input_filename',input_filename)
            df_filtered.to_csv(output_filename, index=False)
            print('Ouput saved to:', output_filename)
        else:
            print('no input_csv, terminating')

        