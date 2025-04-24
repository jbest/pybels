import os
from datetime import datetime
import time
import sys
import re
import multiprocessing as mp
from functools import partial
from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz


# [Previous helper functions remain the same]
def extract_compass_direction(locality):
    if not isinstance(locality, str):
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

def extract_distance(locality):
    if not isinstance(locality, str):
        return None, None
    
    distance_pattern = r'(\d+(\.\d+)?)\s*(km|kilometers|miles|mi|meters|m|feet|ft|yards|yd)'
    
    match = re.search(distance_pattern, locality)
    if match:
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

def compare_fields(field1, field2, method='fuzzy', threshold=80, compass1=None, compass2=None, distance1=None, distance2=None):
    locality_match = False
    if method == 'fuzzy':
        locality_match = fuzz.token_sort_ratio(field1, field2) >= threshold
    elif method == 'cosine':
        vectorizer = TfidfVectorizer().fit_transform([field1, field2])
        cosine_sim = cosine_similarity(vectorizer[0:1], vectorizer[1:2])
        locality_match = cosine_sim[0][0] >= threshold / 100
    
    compass_match = compass1 == compass2
    distance_match = distance1 == distance2
    
    return locality_match and compass_match and distance_match

def find_potential_duplicates(df, similarity_threshold, method='fuzzy', county_name=None):
    df = df.reset_index(drop=True)
    groups = []
    group_id = 1
    assigned_groups = [-1] * len(df)
    
    print(f"County: {county_name}")
    print(f"DataFrame length: {len(df)}")
    print(f"Assigned groups length: {len(assigned_groups)}")
    
    for i, row1 in df.iterrows():
        if i >= len(assigned_groups):
            break
            
        if assigned_groups[i] != -1:
            continue
            
        current_group = [i]
        assigned_groups[i] = group_id
        
        compass1 = extract_compass_direction(row1['locality'])
        distance1, unit1 = extract_distance(row1['locality'])
        
        df.at[i, 'compassDirection'] = compass1
        df.at[i, 'distance'] = distance1
        df.at[i, 'distanceUnit'] = unit1
        
        for j, row2 in df.iterrows():
            if j >= len(assigned_groups):
                break
                
            if i >= j or assigned_groups[j] != -1:
                continue
            
            compass2 = extract_compass_direction(row2['locality'])
            distance2, unit2 = extract_distance(row2['locality'])
            
            if unit1 == unit2:
                if compare_fields(row1['locality'], row2['locality'], method, similarity_threshold,
                                compass1=compass1, compass2=compass2,
                                distance1=(distance1, unit1), distance2=(distance2, unit2)):
                    current_group.append(j)
                    assigned_groups[j] = group_id
                    
                    df.at[j, 'compassDirection'] = compass2
                    df.at[j, 'distance'] = distance2
                    df.at[j, 'distanceUnit'] = unit2
        
        groups.append(current_group)
        group_id += 1

    return df, groups, assigned_groups, group_id

def process_county(county_data):
    """
    Process a single county's data. This function will be run in parallel.
    
    Args:
        county_data (tuple): (county_name, county_df, similarity_threshold)
        
    Returns:
        tuple: (county_name, processed_df, groups, group_assignments, last_group_id)
    """
    county_name, county_df, similarity_threshold = county_data
    print(f"Processing county: {county_name}")
    print(f"Number of records: {len(county_df)}")
    
    processed_df, groups, group_assignments, last_group_id = find_potential_duplicates(
        county_df,
        similarity_threshold,
        method='fuzzy',
        county_name=county_name
    )
    
    # Explicitly set the Group_ID column in the processed DataFrame
    processed_df['Group_ID'] = group_assignments
    
    return county_name, processed_df, groups, group_assignments, last_group_id

def sanitize_filename(filename, replacement=''):
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1F]', replacement, filename)
    filename = filename.strip()
    filename = filename[:255]
    return filename

def main():
    # Read the input file
    csv_file = '/media/jbest/data3/BRIT_git/TORCH_georeferencing/data/Texas/panhandle/panhandle_test/occurrences_BELS_metrics.tab'
    df = pd.read_csv(csv_file, low_memory=False, sep='\t')
    
    bels_location_ids = df['bels_location_id'].unique()
    print('bels_location_ids unique count:', len(bels_location_ids))
    
    counties = df['county'].unique()
    print('counties unique count:', len(counties))
    print("Initial DataFrame info:")
    print(df.info())
    print("\nUnique counties:", counties)
    
    # Create results directory
    results_dir = Path('BELS_Grouper_results')
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Prepare data for parallel processing
    similarity_threshold = 95
    county_data = []
    for county in counties:
        county_df = df[df['county'] == county].copy()
        county_data.append((county, county_df, similarity_threshold))
    
    # Set up multiprocessing
    num_processes = mp.cpu_count() - 1  # Leave one core free
    print(f"Using {num_processes} processes")
    
    # Process counties in parallel
    with mp.Pool(processes=num_processes) as pool:
        results = pool.map(process_county, county_data)
    
    # Save results for each county
    for county_name, processed_df, groups, group_assignments, last_group_id in results:
        county_sanitized = sanitize_filename(county_name)
        filename = county_sanitized + '_BELS_Grouper.csv'
        results_path = results_dir / filename
        processed_df.to_csv(results_path, sep='\t', index=False)
        print(f"Saved results for {county_name} to {results_path}")

if __name__ == "__main__":
    main()
