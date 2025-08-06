import pandas as pd

def left_join_with_condition(df1, df2, left_on, right_on, filter_by = 'claim_date_of_service', normalize_valid_from = False):
    '''
    this method helps to perform SQL-like left join with additional datetime-based conditions. 
    it's important to double check for duplicated column names since it will lead to index error during pd.concat().
    - normalize_valid_from: bool, default False
        If True, normalizes the filter_by column in df1 to midnight (00:00:00) and adds 12 hours for the comparison.
        Useful when matching prices uploaded with standardized midday timestamps (e.g. YYYY-mm-dd 10:00:00).
    '''
    df1.reset_index(inplace=True, drop=True)
    df1['row_number'] = df1.reset_index().index
    merged_df = pd.merge(df1, df2, how = 'left', left_on = left_on, right_on = right_on)
    if filter_by == 'claim_date_of_service':
        merged_df = merged_df[(merged_df[filter_by] + pd.Timedelta(hours=12) >= merged_df.valid_from_y) & (merged_df[filter_by] + pd.Timedelta(hours=12) < merged_df.valid_to_y)].rename(columns={'valid_from_x':'valid_from','valid_to_x':'valid_to'}).drop(columns=['valid_from_y','valid_to_y'])
    elif not normalize_valid_from and filter_by != 'claim_date_of_service':
        merged_df = merged_df[(merged_df[filter_by] >= merged_df.valid_from_y) & (merged_df[filter_by] < merged_df.valid_to_y)].rename(columns={'valid_from_x':'valid_from','valid_to_x':'valid_to'}).drop(columns=['valid_from_y','valid_to_y'])    
    elif normalize_valid_from and filter_by != 'claim_date_of_service':  # this approach transform real timestamp e.g. 2025-01-01 23:51:19.333 to 2025-01-01 12:00:00; it's important to join correct price on specific date since we upload prices with YYYY-mm-dd 10:00:00
        merged_df = merged_df[(merged_df[filter_by].dt.normalize() + pd.Timedelta(hours=12) >= merged_df.valid_from_y) & (merged_df[filter_by].dt.normalize() + pd.Timedelta(hours=12) < merged_df.valid_to_y)].rename(columns={'valid_from_x':'valid_from','valid_to_x':'valid_to'}).drop(columns=['valid_from_y','valid_to_y'])
    else:
        raise ValueError(f"Unexpected filter_by value: {filter_by} or invalid combination with normalize_valid_from = {normalize_valid_from}")
    merged_df = merged_df.rename(columns={f'{right_on}_x': right_on}).drop(columns={f'{right_on}_y'}, errors='ignore')
    df1 = pd.concat([merged_df,df1[~df1['row_number'].isin(merged_df.row_number)]])
    return df1.drop(columns=['row_number'])
