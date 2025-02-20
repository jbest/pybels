import os
from pathlib import Path
from datetime import datetime
import time
import sys
import re

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz


def arg_setup():
    # set up argument parser
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=True, \
        help="Input directory path for a single TAB file.")
    ap.add_argument("-v", "--verbose", action="store_true", \
        help="Detailed output.")
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
def assign_sub_groups(df, eventdate_tolerance=3, recordnumber_tolerance=5, habitat_similarity_threshold=80, handle_null_recordnumber='0', handle_null_eventdate='0'):
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

# Function to check if all latitudes and longitudes in a group are identical
def has_identical_coords(group):
    """Check if all records in the group have identical latitude and longitude."""
    return (group['decimalLatitude'].nunique() == 1) and (group['decimalLongitude'].nunique() == 1)


# Function to count allowed institutions in each group
def count_allowed_institutions(df, allowed_institutions):
    """Count the number of records in each group that have institutionCode in the allowed list."""
    allowed_institutions = [inst.strip() for inst in allowed_institutions.split(',')]
    # Create a new column that checks if the institutionCode is in the allowed_institutions list
    df['is_allowed_institution'] = df['institutionCode'].apply(lambda x: x in allowed_institutions)
    
    # Group by 'Group_ID' and count the allowed institutionCode matches
    group_counts = df.groupby('Group_ID')['is_allowed_institution'].sum().reset_index()
    group_counts.rename(columns={'is_allowed_institution': 'allowed_institution_count'}, inplace=True)
    
    return group_counts

# Modify the save_filtered_groups_to_csv function to include compassDirection and distance
def save_filtered_groups_to_csv(input_filename, df, group_assignments, sub_group_assignments, min_size, export_columns, allowed_institutions):
    df['Group_ID'] = group_assignments  # Add the group ID to the DataFrame
    df['Sub_Group_ID'] = sub_group_assignments  # Add the sub-group ID to the DataFrame
    
    # Add compassDirection and distance to export_columns if not already included
    if 'compassDirection' not in export_columns:
        export_columns.append('compassDirection')
    if 'distance' not in export_columns:
        export_columns.append('distance')
    if 'distanceUnit' not in export_columns:
        export_columns.append('distanceUnit')
    
    # Filter to only include groups that have more than the specified number of records
    group_counts = df['Group_ID'].value_counts()
    large_groups = group_counts[group_counts > min_size].index
    
    # Exclude records with blank or null localities and identical decimalLatitude and decimalLongitude
    filtered_df = df[
        df['Group_ID'].isin(large_groups) & 
        df['locality'].notnull() & df['locality'].str.strip().ne('')
    ]
    
    # Filter out groups where all latitudes and longitudes are identical
    filtered_df = filtered_df.groupby('Group_ID').filter(lambda x: not has_identical_coords(x))
    
    # Filter by allowed institutionCode
    filtered_df = filter_by_institution(filtered_df, allowed_institutions)
    
    if not filtered_df.empty:
        # Count the number of allowed institutions in each group
        allowed_institution_counts = count_allowed_institutions(filtered_df, allowed_institutions)
        
        # Merge the counts back to the filtered DataFrame
        filtered_df = filtered_df.merge(allowed_institution_counts, on='Group_ID', how='left')
        
        # Reorder columns based on the configuration
        if export_columns:
            filtered_df = filtered_df[export_columns + ['allowed_institution_count']]
        
        # Sort by allowed institution count, then by Group_ID, Sub_Group_ID, and eventDate
        if 'eventDate' in filtered_df.columns:
            filtered_df.sort_values(by=['allowed_institution_count', 'Group_ID', 'Sub_Group_ID', 'eventDate'], ascending=[False, True, True, True], inplace=True)
        else:
            filtered_df.sort_values(by=['allowed_institution_count', 'Group_ID', 'Sub_Group_ID'], ascending=[False, True, True], inplace=True)
        
        # Automatically create the output filename by appending "-groups.csv"
        output_filename = input_filename.replace('.csv', '-groups.csv')
        
        # Save the filtered DataFrame to the output file
        filtered_df.to_csv(output_filename, index=False)
        print(f"Group data saved to {output_filename}")
    else:
        print(f"No groups found with more than {min_size} records and non-blank localities.")

import re

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


# Main function
def main():
    # Load the configuration and fields from export_config.txt
    """
    config, export_columns = load_export_config()
    
    if config is None or export_columns is None:
        return
    """

    # Load CSV files from the selected folder
    #csv_files, folder_path = load_csv_files_from_folder()
    # input file must already have BELS metrics (bels string, group id, etc.)
    #csv_file = '/media/jbest/data3/BRIT_git/TORCH_georeferencing/data/Texas/panhandle/panhandle_test/occurrences_BELS_metrics.tab'
    csv_file = '/media/jbest/data3/BRIT_git/TORCH_georeferencing/data/Texas/panhandle/panhandle_test/occurrences_BELS_metrics_sample.tab'
    
    if csv_file:
        df = pd.read_csv(csv_file, low_memory=False, sep='\t')
        #df.sort_values('B').groupby('A').first()
        #print(df)
        #print(df.columns.to_list())
        #df_bels_reps = df.groupby('bels_location_id').first()
        #print(df_bels_reps)
        bels_location_ids = df['bels_location_id'].unique()
        print('bels_location_ids unique count:', len(bels_location_ids))
        #TODO Normalize county names
        counties = df['county'].unique()
        print('counties unique count:', len(counties))

        
        print("Initial DataFrame info:")
        print(df.info())
        print("\nUnique counties:", counties)
        for county in counties:
            df_bels_county = df[df['county'] == county]
            print(f"Processing county: {county}")
            print(f"Number of records: {len(df_bels_county)}")
            print(f"DataFrame shape: {df_bels_county.shape}")

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
            
            
            county_sanitized = sanitize_filename(county)
            filename = county_sanitized + '_BELS_Grouper.csv'
            results_dir = Path('BELS_Grouper_results')
            results_dir.mkdir(parents=True, exist_ok=True)
            results_path = results_dir / filename
            df_bels_county.to_csv(results_path, sep='\t', index=False)

    else:
        print("No valid CSV files to process.")

if __name__ == "__main__":
    main()
