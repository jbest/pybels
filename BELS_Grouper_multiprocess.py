import os
from pathlib import Path
from datetime import datetime
import time
import sys
import re
import argparse
import multiprocessing as mp
from functools import partial

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz
import time


def arg_setup():
    # set up argument parser
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input_path", required=True, \
        help="Input directory path for a single TAB file.")
    ap.add_argument("-v", "--verbose", action="store_true", \
        help="Detailed output.")
    ap.add_argument("-p", "--processes", type=int, default=None, \
        help="Number of processes to use. Default is number of CPU cores -1.")
    args = vars(ap.parse_args())
    return args

# Function to extract and normalize compass directions from the locality string
def extract_compass_direction(locality):
    if not isinstance(locality, str):  # Ensure locality is a string
        return None
    
    compass_patterns = {
        'N': r'\b[Nn]orth\b|\b[Nn]\b',
        'NE': r'\b[Nn]orth[eE]ast\b|\b[Nn][Ee]\b',
        'E': r'\b[Ee]ast\b|\b[Ee]\b',
        'SE': r'\b[Ss]outh[eE]ast\b|\b[Ss][Ee]\b',
        'S': r'\b[Ss]outh\b|\b[Ss]\b',
        'SW': r'\b[Ss]outh[wW]est\b|\b[Ss][Ww]\b',
        'W': r'\b[Ww]est\b|\b[Ww]\b',
        'NW': r'\b[Nn]orth[wW]est\b|\b[Nn][Ww]\b'
    }
    
    for direction, pattern in compass_patterns.items():
        if re.search(pattern, locality):
            return direction
    return None

# Function to extract and normalize distances from the locality string
def extract_distance(locality):
    if not isinstance(locality, str):  # Ensure locality is a string
        return None, None
    
    # Regular expression to find distances with units like km, miles, meters, etc.
    distance_pattern = r'(\d+(\.\d+)?)\s*(km|kilometers|miles|mi|meters|m|feet|ft|yards|yd)'
    
    match = re.search(distance_pattern, locality)
    if match:
        # Extract the number and unit, and normalize the unit
        distance_value = float(match.group(1))
        unit = match.group(3).lower()
        
        if unit in ['kilometers', 'km']:
            unit = 'km'
        elif unit in ['miles', 'mi']:
            unit = 'miles'
        elif unit in ['meters', 'm']:
            unit = 'm'
        elif unit in ['feet', 'ft']:
            unit = 'ft'
        elif unit in ['yards', 'yd']:
            unit = 'yd'
        
        return distance_value, unit
    return None, None

# Function to calculate similarity between two text fields
def compare_fields(field1, field2, method='fuzzy', threshold=80, compass1=None, compass2=None, distance1=None, distance2=None):
    locality_match = False
    if method == 'fuzzy':
        locality_match = fuzz.token_sort_ratio(field1, field2) >= threshold
    elif method == 'cosine':
        vectorizer = TfidfVectorizer().fit_transform([field1, field2])
        cosine_sim = cosine_similarity(vectorizer[0:1], vectorizer[1:2])
        locality_match = cosine_sim[0][0] >= threshold / 100
    
    # Check if compass directions and distances match exactly
    compass_match = compass1 == compass2
    distance_match = distance1 == distance2
    
    return locality_match and compass_match and distance_match

def find_potential_duplicates(df, similarity_threshold, method='fuzzy', county_name=None):
    # Reset the index of the DataFrame to ensure we start from 0
    df = df.reset_index(drop=True)
    
    groups = []
    group_id = 1
    
    # Initialize assigned_groups and print lengths for debugging
    assigned_groups = [-1] * len(df)
    print(f"County: {county_name}")
    print(f"DataFrame length: {len(df)}")
    print(f"Assigned groups length: {len(assigned_groups)}")
    
    for i, row1 in df.iterrows():
        #print(f"Processing row i={i}")
        if i >= len(assigned_groups):
            #print(f"WARNING: i={i} is >= assigned_groups length {len(assigned_groups)}")
            break
            
        if assigned_groups[i] != -1:
            continue
            
        current_group = [i]
        assigned_groups[i] = group_id
        
        # Extract compass direction and distance for this record
        compass1 = extract_compass_direction(row1['locality'])
        distance1, unit1 = extract_distance(row1['locality'])
        
        # Save the extracted data to the DataFrame
        df.at[i, 'compassDirection'] = compass1
        df.at[i, 'distance'] = distance1
        df.at[i, 'distanceUnit'] = unit1
        
        for j, row2 in df.iterrows():
            #print(f"  Comparing with j={j}")
            if j >= len(assigned_groups):
                #print(f"  WARNING: j={j} is >= assigned_groups length {len(assigned_groups)}")
                break
                
            if i >= j or assigned_groups[j] != -1:
                continue
            
            # Extract compass direction and distance for the second record
            compass2 = extract_compass_direction(row2['locality'])
            distance2, unit2 = extract_distance(row2['locality'])
            
            # Only compare if both records have matching distance units
            if unit1 == unit2:
                if compare_fields(row1['locality'], row2['locality'], method, similarity_threshold,
                                compass1=compass1, compass2=compass2,
                                distance1=(distance1, unit1), distance2=(distance2, unit2)):
                    current_group.append(j)
                    assigned_groups[j] = group_id
                    
                    # Save the extracted data to the DataFrame
                    df.at[j, 'compassDirection'] = compass2
                    df.at[j, 'distance'] = distance2
                    df.at[j, 'distanceUnit'] = unit2
        
        groups.append(current_group)
        group_id += 1

    return groups, assigned_groups, group_id

# Function to assign sub-groups based on similar eventDate, recordNumber, and habitat values
def assign_sub_groups(df, eventdate_tolerance=3, recordnumber_tolerance=5, habitat_similarity_threshold=95, handle_null_recordnumber='0', handle_null_eventdate='0'):
    # Reset the index of the DataFrame to ensure we start from 0
    df = df.reset_index(drop=True)

    sub_groups = [-1] * len(df)
    sub_group_id = 1

    #print('assign_sub_groups...')
    
    # Convert recordNumber to numeric for comparison, setting non-convertible values to NaN
    df['recordNumber'] = pd.to_numeric(df['recordNumber'], errors='coerce')
    
    for group_id in df['Group_ID'].unique():
        #print('finding sub_groups for Group_ID:', group_id)
        group_df = df[df['Group_ID'] == group_id]
        for i, row1 in group_df.iterrows():
            if sub_groups[i] != -1:  # Skip if already assigned a sub-group
                continue
            sub_group = [i]
            sub_groups[i] = sub_group_id
            
            for j, row2 in group_df.iterrows():
                if i >= j or sub_groups[j] != -1:
                    continue
                
                # Safely parse eventDate (set to a default if invalid)
                try:
                    date1 = datetime.strptime(row1['eventDate'], '%Y-%m-%d') if pd.notna(row1['eventDate']) else None
                    date2 = datetime.strptime(row2['eventDate'], '%Y-%m-%d') if pd.notna(row2['eventDate']) else None
                except ValueError:
                    date1, date2 = None, None
                
                # Handle eventDate comparison based on the config setting
                if date1 is None or date2 is None:
                    if handle_null_eventdate == '0':
                        date_diff = 0  # Treat nulls as no difference
                    else:
                        date_diff = float('inf')  # Treat nulls as infinite difference
                else:
                    date_diff = abs((date1 - date2).days)

                # Handle recordNumber comparison based on the config setting
                if pd.isna(row1['recordNumber']) or pd.isna(row2['recordNumber']):
                    if handle_null_recordnumber == '0':
                        record_diff = 0  # Treat nulls as no difference
                    else:
                        record_diff = float('inf')  # Treat nulls as infinite difference
                else:
                    record_diff = abs(row1['recordNumber'] - row2['recordNumber'])
                
                # Handle habitat similarity
                habitat_similarity = fuzz.token_sort_ratio(row1['habitat'], row2['habitat']) if pd.notna(row1['habitat']) and pd.notna(row2['habitat']) else 0
                habitat_match = habitat_similarity >= habitat_similarity_threshold
                
                # Check if both date, record number differences are within tolerance, and habitat is similar
                if date_diff <= eventdate_tolerance and record_diff <= recordnumber_tolerance and habitat_match:
                    sub_group.append(j)
                    sub_groups[j] = sub_group_id
            
            sub_group_id += 1  # Increment sub-group ID for the next sub-group
    
    return sub_groups

def add_unique_id_count(df):
    """
    Add a column with the count of unique bels_id values for each grouper_id.
    Modifies the DataFrame in place.
    
    Parameters:
    df (pandas.DataFrame): DataFrame with bels_id and grouper_id columns
    """
    # Group by grouper_id and count unique bels_id values
    unique_counts = df.groupby('Group_ID')['bels_location_id'].nunique()
    
    # Map these counts back to the original DataFrame, modifying it in place
    df['id_score'] = df['Group_ID'].map(unique_counts)

# Function to normalize county names
def normalize_county_name(county_name):
    if isinstance(county_name, str):
        county_name = county_name.lower().strip()
        county_name = re.sub(r'(county|co\.?|[\?\.])$', '', county_name).strip()
        return county_name.title()
    else:
        return ''

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

# New function to process a single county
def process_county(county, df, df_bels_unique, results_dir):
    try:
        print(f"Processing county: {county} (PID: {os.getpid()})")
        df_bels_county = df_bels_unique[df_bels_unique['county'] == county]
        print(f"Number of records for {county}: {len(df_bels_county)}")
        
        if len(df_bels_county) == 0:
            print(f"No records for county {county}, skipping.")
            return
            
        similarity_threshold = 95
        
        groups, group_assignments, last_group_id = find_potential_duplicates(
            df_bels_county, 
            similarity_threshold, 
            method='fuzzy', 
            county_name=county
        )

        # Add groups to dataframe
        df_bels_county['Group_ID'] = group_assignments

        # Assign sub-groups based on eventDate, recordNumber, and habitat similarity
        sub_group_assignments = assign_sub_groups(df_bels_county)
        df_bels_county['Sub_Group_ID'] = sub_group_assignments

        df_bels_county_all = df[df['county'] == county]
        
        # Add location score
        add_unique_id_count(df_bels_county)

        # merge results of a filtered BELS_Grouper dataset with original ungrouped
        df_county_merged = pd.merge(
            df_bels_county_all,
            df_bels_county[['bels_location_id', 'Group_ID','Sub_Group_ID','id_score']],
            on='bels_location_id',
            how='left'
        )

        county_sanitized = sanitize_filename(county)
        filename = county_sanitized + '_BELS_Grouper.tsv'
        results_dir.mkdir(parents=True, exist_ok=True)
        results_path = results_dir / filename
        df_county_merged.to_csv(results_path, sep='\t', index=False)
        
        print(f"Completed processing county: {county}")
        
        return county, len(df_county_merged)
    except Exception as e:
        print(f"Error processing county {county}: {str(e)}")
        return county, f"Error: {str(e)}"

# Main function with multiprocessing
def main():
    # set up argparse
    args = arg_setup()
    verbose = args['verbose']
    input_path = args['input_path']
    num_processes = args['processes']

    if num_processes is None:
        num_processes = mp.cpu_count()-1
    
    print(f"Using {num_processes} processes")

    # Load the data
    csv_file = input_path
    
    if not os.path.exists(csv_file):
        print(f"Input file not found: {csv_file}")
        return
        
    print(f"Loading data from {csv_file}...")
    
    # TSV import
    df = pd.read_csv(csv_file, low_memory=False, sep='\t')
    
    bels_location_ids = df['bels_location_id'].unique()
    print('bels_location_ids unique count:', len(bels_location_ids))

    counties = df['county'].unique()
    print('counties unique count:', len(counties))

    print("Initial df DataFrame info:")
    print(df.info())
    print("\nUnique counties:", counties)
    
    print('Reducing records to unique BELS values...')
    # Make a representative datset with one from each BELS location
    df_bels_unique = df.groupby('bels_location_id').first().reset_index()
    print("Reduced df_bels_unique info:")
    print(df_bels_unique.info())
    
    # Create results directory
    results_dir = Path('BELS_Grouper_results')
    
    # Set up multiprocessing pool
    pool = mp.Pool(processes=num_processes)
    
    # Create a partial function with fixed arguments
    process_county_partial = partial(process_county, df=df, df_bels_unique=df_bels_unique, results_dir=results_dir)
    
    # Process counties in parallel
    results = pool.map(process_county_partial, counties)
    
    # Close the pool
    pool.close()
    pool.join()
    
    # Report results
    #print(results)
    # remove None results
    results = [x for x in results if x is not None]
    print("\nProcessing summary:")
    for county, result in results:
        if isinstance(result, int):
            print(f"County: {county} - Processed {result} records")
        else:
            print(f"County: {county} - {result}")

if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))