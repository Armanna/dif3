def filter_walgreens_claims(df, period_start, period_end):
  """
  we need to filter out claims in accordance with cst6cdt time zone for Walgreens only after valid_from and valid_to has been transformed to cst6cdt
  """
  df = df[(df.valid_from.dt.date >= period_start.date()) & (df.valid_from.dt.date <= period_end.date()) & (df.valid_to.dt.date > period_end.date())]
  return df

def add_walgreens_reversal_indicator(df):
    df['reversal indicator'] = df.transaction_code
    return df
    
