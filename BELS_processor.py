from pathlib import Path

import pandas as pd

data_path = '/mnt/DATA3-4TB/BRIT_git/TORCH_georeferencing/data/TORCH-data_snapshots_TX_OK_2024-12-06/'
input_file = 'torch_bels_locs.tsv' # stored in data_path


def analyze_locations_v1(df):
    """
    Creates location IDs and analyzes location groups.
    
    Parameters:
    df (pd.DataFrame): DataFrame with 'bels_location_string' and coordinate columns
    
    Returns:
    pd.DataFrame: DataFrame with location IDs and group statistics
    """
    # Create location IDs
    unique_locations = df['bels_location_string'].unique()
    location_to_id = {loc: idx for idx, loc in enumerate(unique_locations)}
    df['bels_location_id'] = df['bels_location_string'].map(location_to_id)
    
    # Calculate stats per location
    location_stats = df.groupby('bels_location_string').agg({
        'decimalLatitude': lambda x: x.notna().sum(),
        'bels_location_string': 'count'
    }).rename(columns={
        'decimalLatitude': 'coordinate_count',
        'bels_location_string': 'bels_group_rec_count'
    })
    
    # Merge stats back to original dataframe
    return df.merge(location_stats, on='bels_location_string')

def analyze_locations(df):
    """
    Creates location IDs, coordinate pair IDs, and analyzes location groups.
    Includes counts of matching coordinate pairs.
    
    Parameters:
    df (pd.DataFrame): DataFrame with 'bels_location_string' and coordinate columns
    
    Returns:
    pd.DataFrame: DataFrame with location IDs, coordinate IDs, and various statistics
    """
    # Create location IDs
    unique_locations = df['bels_location_string'].unique()
    location_to_id = {loc: idx for idx, loc in enumerate(unique_locations)}
    df['bels_location_id'] = df['bels_location_string'].map(location_to_id)
    
    # Create unique IDs for distinct coordinate pairs
    # First, create a tuple of coordinates to use as a key
    df['coord_pair'] = df.apply(
        lambda row: (row['decimalLongitude'], row['decimalLatitude'])
        if pd.notna(row['decimalLongitude']) and pd.notna(row['decimalLatitude'])
        else None,
        axis=1
    )
    
    # Create mapping of unique coordinate pairs to IDs
    unique_coords = df['coord_pair'].dropna().unique()
    coord_to_id = {coord: idx for idx, coord in enumerate(unique_coords)}
    
    # Map the IDs back to the dataframe and convert to integer
    df['coord_group_id'] = df['coord_pair'].map(coord_to_id).astype('Int64')
    
    # Calculate count of matching coordinate pairs and convert to integer
    coord_counts = df['coord_pair'].value_counts().to_dict()
    df['coord_group_match_count'] = df['coord_pair'].map(coord_counts).astype('Int64')
    
    # Drop the temporary coord_pair column
    df = df.drop('coord_pair', axis=1)
    
    # Calculate stats per location
    location_stats = df.groupby('bels_location_string').agg({
        'decimalLatitude': lambda x: x.notna().sum(),
        'bels_location_string': 'count'
    }).rename(columns={
        'decimalLatitude': 'bels_group_coord_count',
        'bels_location_string': 'bels_group_rec_count'
    })
    
    # Merge stats back to original dataframe
    return df.merge(location_stats, on='bels_location_string')

def summarize_locations_by_region(df):
    """
    Generates location group statistics for each state/county combination.
    """
    def get_stats(group):
        total_groups = group['bels_location_id'].nunique()
        groups_with_coords = group.groupby('bels_location_id')['bels_group_coord_count'].first()
        total_records = len(group)
        records_with_coords = group['decimalLatitude'].notna().sum()
        
        return pd.Series({
            'total_location_groups': total_groups,
            'groups_with_coordinates': (groups_with_coords > 0).sum(),
            'groups_without_coordinates': (groups_with_coords == 0).sum(),
            'total_records': total_records,
            'records_with_coordinates': records_with_coords,
            'records_without_coordinates': total_records - records_with_coords
        })
    
    return df.groupby(['stateProvince', 'county']).apply(get_stats, include_groups=False).reset_index()

if __name__ == "__main__":
    # Input DataFrame
    #df_occ = pd.read_csv('torch_bels_locs_SAMPLE.tsv', low_memory=False, sep='\t')
    #df_occ = pd.read_csv('data_tsv/torch_bels_locs.tsv', low_memory=False, sep='\t')
    input_path = Path(data_path) / input_file
    metrics_path = Path(data_path) / 'torch_bels_metrics.tsv'
    summary_path = Path(data_path) / 'torch_bels_summary.tsv'
    print('Loading:', input_path)
    df_occ = pd.read_csv(input_path, low_memory=False, sep='\t')
    
    # Process the DataFrame
    print('Generating BELS metrics...')
    df_occ_bels_metrics = analyze_locations(df_occ)
    print('Saving BELS metrics:', metrics_path)
    df_occ_bels_metrics.to_csv(metrics_path, sep='\t', index=False)

    df_summary = summarize_locations_by_region(df_occ_bels_metrics)
    print('Saving BELS summary:', summary_path)
    df_summary.to_csv(summary_path, sep='\t', index=False)
