import pandas as pd
from datetime import datetime as dt


def calculate_start_end_dates(df, period_start, period_end):
    df['start_date'] = df.apply(lambda df_row:
        df_row.period_start if df_row.valid_from_chain_rates < df_row.period_start else
        df_row.valid_from_chain_rates, axis=1)
    df['end_date'] = df.apply(lambda df_row:
        df_row.period_end if df_row.valid_to_chain_rates > df_row.period_end else
        df_row.valid_to_chain_rates, axis=1)
    df['period_start'],df['period_end'] = period_start.strftime('%m/%d/%Y'), period_end.strftime('%m/%d/%Y')
    return df.drop(columns=['valid_from_chain_rates','valid_to_chain_rates'])
  
def set_net_invoice_number_and_date(df, period_start, period_end, net_number, due_date, invoice_static_number):
  df['net'] = net_number
  df['invoice_number'] = invoice_static_number + ((dt.now() - dt(2021, 4, 1)).days // 7) # random number which won't be duplicated
  df['period_start'],df['period_end'] = period_start.strftime('%m/%d/%Y'), period_end.strftime('%m/%d/%Y')
  df['invoice_date'],df['due_date'] = (period_end + pd.Timedelta(days=1)).strftime('%m/%d/%Y'), (period_end + pd.Timedelta(days=due_date)).strftime('%m/%d/%Y')
  return df
