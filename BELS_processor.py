import pandas as pd

def analyze_locations(df):
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

def summarize_locations_by_region(df):
    """
    Generates location group statistics for each state/county combination.
    """
    def get_stats(group):
        total_groups = group['bels_location_id'].nunique()
        groups_with_coords = group.groupby('bels_location_id')['coordinate_count'].first()
        total_records = len(group)
        
        return pd.Series({
            'total_location_groups': total_groups,
            'groups_with_coordinates': (groups_with_coords > 0).sum(),
            'groups_without_coordinates': (groups_with_coords == 0).sum(),
            'total_records': total_records
        })
    
    return df.groupby(['stateProvince', 'county']).apply(get_stats, include_groups=False).reset_index()

if __name__ == "__main__":
    # Input DataFrame
    df_occ = pd.read_csv('torch_bels_locs_SAMPLE.tsv', low_memory=False, sep='\t')
    
    # Process the DataFrame
    df_occ_bels_metrics = analyze_locations(df_occ)
    
    # Display all columns
    #pd.set_option('display.max_columns', None)
    #print("\nProcessed DataFrame with coordinate counts:")
    #print(result)
    df_occ_bels_metrics.to_csv('torch_bels_metrics.tsv', sep='\t')

    df_summary = summarize_locations_by_region(df_occ_bels_metrics)
    df_summary.to_csv('torch_bels_summary.tsv', sep='\t')
