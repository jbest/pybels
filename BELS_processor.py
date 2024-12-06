#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Generated using Claude.ai
#https://claude.ai/chat/18fa6039-d306-449d-958b-08ad823fe977


# In[12]:


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


# In[22]:


def summarize_locations_by_region_v1(df):
    """
    Generates location group statistics for each state/county combination.
    /tmp/ipykernel_516747/3985458850.py:15: DeprecationWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  return df.groupby(['stateProvince', 'county']).apply(get_stats).reset_index()
    """
    def get_stats(group):
        total_groups = group['bels_location_id'].nunique()
        groups_with_coords = group.groupby('bels_location_id')['coordinate_count'].first()
        
        return pd.Series({
            'total_location_groups': total_groups,
            'groups_with_coordinates': (groups_with_coords > 0).sum(),
            'groups_without_coordinates': (groups_with_coords == 0).sum()
        })
    
    return df.groupby(['stateProvince', 'county']).apply(get_stats).reset_index()


# In[30]:


def summarize_locations_by_region(df):
    """
    Generates location group statistics for each state/county combination.
    """
    def get_stats(group):
        total_groups = group['bels_location_id'].nunique()
        groups_with_coords = group.groupby('bels_location_id')['coordinate_count'].first()
        
        return pd.Series({
            'total_location_groups': total_groups,
            'groups_with_coordinates': (groups_with_coords > 0).sum(),
            'groups_without_coordinates': (groups_with_coords == 0).sum()
        })
    
    return df.groupby(['stateProvince', 'county']).apply(get_stats, include_groups=False).reset_index()


# In[17]:


# Example usage
if __name__ == "__main__":
    # Input DataFrame
    df_occ = pd.read_csv('torch_bels_locs_SAMPLE.tsv', low_memory=False, sep='\t')
    
    # Process the DataFrame
    #result = add_coordinate_counts(data)
    result = analyze_locations(df_occ)
    
    # Display all columns
    pd.set_option('display.max_columns', None)
    print("\nProcessed DataFrame with coordinate counts:")
    print(result)


# In[18]:


result
#df_sorted = result.sort_values(by=['coordinate_count'], ascending=False)
#df_sorted

# After running analyze_locations
print(result.columns)  # This will show all column names
#print(columns[['bels_location_string', 'bels_location_id', 'bels_group_rec_count']].head())  # Show sample dataresult
# In[31]:


df_summary = summarize_locations_by_region(result)


# In[32]:


df_summary


# In[ ]:




