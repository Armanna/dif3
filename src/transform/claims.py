import pandas as pd
import numpy as np
import decimal
from decimal import Decimal, ROUND_HALF_DOWN, ROUND_HALF_UP
from transform import utils, claims, invoices
from sources import claims as sources_claims

def build_default_transactions_dataset(raw_claims_df, source_dict, chain_name, chain_class, period_start, period_end, **kwargs):
    raw_cvs_quarterly_df = sources_claims.join_default_source_dataframes(raw_claims_df, source_dict)
    raw_cvs_quarterly_df = claims.filter_by_binnumber_authnumber(raw_cvs_quarterly_df, chain_class.BIN_NUMBERS, chain_class.CHAIN_CODES) # filter dataframe by bin_number and authorization_number
    raw_cvs_quarterly_df = utils.cast_cents_to_dollars(raw_cvs_quarterly_df, column_names=['usual_and_customary_charge', 'percentage_sales_tax_amount_paid', 'total_paid_response', 'patient_pay_resp', 'ingredient_cost_paid_resp', 'dispensing_fee_paid_resp','awp', 'mac', 'wac', 'nadac', 'gpi_nadac'], add_fill_reversal_sign=False)
    raw_cvs_quarterly_df['drug_type'] = chain_class.add_brand_generic_indicator(raw_cvs_quarterly_df)
    raw_cvs_quarterly_df['price_basis'] = claims.determine_basis_of_rembursment_abbreviation(raw_cvs_quarterly_df['basis_of_reimbursement_determination_resp'])
    raw_cvs_quarterly_df['reconciliation_price_basis'] = chain_class.define_reconciliation_price_basis(raw_cvs_quarterly_df)
    raw_cvs_quarterly_df['transaction_type'] = claims.define_transaction_type(raw_cvs_quarterly_df)
    transactions_df = sources_claims.join_chain_rates(raw_cvs_quarterly_df, source_dict['chain_rates'][source_dict['chain_rates']['chain_name'] == chain_name], column_to_merge='drug_type', **kwargs).rename(columns={'bin_number_x':'bin_number', 'bin_number_y':'bin_number_chain_rates','partner_y':'partner_chain_rates','partner_x':'partner_carholders'})
    transactions_df['fill_reversal_indicator'] = claims.add_fills_reversals_indicator(transactions_df, period_start, period_end)
    transactions_df['reversal indicator'] = claims.define_reversal_indicator(transactions_df, period_start, period_end, valid_from_column='valid_from')
    costs_df = claims.calculate_contracted_and_full_cost(transactions_df) # this function add two new columns: contracted_cost and full_cost
    transactions_df = transactions_df.join(costs_df)
    for col_name, pd_series in {'dispensing_fee_paid_resp': transactions_df['dispensing_fee_paid_resp'], 'ingredient_cost_paid_resp':transactions_df['ingredient_cost_paid_resp'], 'total_paid_response': -transactions_df['total_paid_response'], 'patient_pay_resp': transactions_df['patient_pay_resp'], 'contracted_cost': transactions_df['contracted_cost'], 'full_cost': transactions_df['full_cost'], 'percentage_sales_tax_amount_paid': transactions_df['percentage_sales_tax_amount_paid'], 'usual_and_customary_charge': transactions_df['usual_and_customary_charge']}.items():
        transactions_df[col_name] = claims.apply_sign_for_fills_and_reversals(transactions_df, pd_series)
    transactions_df = claims.set_precision_for_numeric_columns(transactions_df)
    transactions_df[['actual effective rate','target effective rate','effective rate variance','ic dollar variance','df dollar variance']] = claims.calculate_effective_rate_and_dollar_variance(transactions_df, price_basis_column='reconciliation_price_basis', **kwargs)
    if hasattr(chain_class, 'set_exclusion_flags'): # if there are any exclusions for specific chain - run function; else - set default values
        transactions_df[['Excluded Claim Indicator', 'Exclusion reason']] = transactions_df.apply(
            chain_class.set_exclusion_flags, axis=1, result_type='expand'
        )
    else:
        transactions_df['Excluded Claim Indicator'] = "N"
        transactions_df['Exclusion reason'] = ""
    return transactions_df

def build_default_invoice_data(raw_claims_df, chain_class, period_start, period_end):
    invoice_claims_df = raw_claims_df.copy()
    invoice_claims_df['period_start'] = period_start
    invoice_claims_df['period_end'] = period_end
    invoice_claims_df['year'] = period_start.year
    invoice_claims_df = invoice_claims_df.rename(columns={'price_basis':'basis_of_reimbursement_source'})
    for column in invoice_claims_df.columns:
        if invoice_claims_df[column].dtype.name == 'category':
            invoice_claims_df[column] = invoice_claims_df[column].cat.remove_unused_categories()
            invoice_claims_df[column] = invoice_claims_df[column].astype('object')
    invoice_grouped_df = invoice_claims_df.groupby(['period_start', 'period_end', 'year', 'transaction_type', 'drug_type', 'basis_of_reimbursement_source'], as_index=False).aggregate({'ingredient_cost_paid_resp':'sum','total_paid_response':'sum','fill_reversal_indicator':'sum'}).rename(columns={'fill_reversal_indicator':'claim_count'})
    raw_invoices_df = invoices.set_net_invoice_number_and_date(invoice_grouped_df, period_start, period_end, chain_class.NET_NUMBER, chain_class.DUE_DATE, chain_class.INVOICE_STATIC_NUMBER)
    invoices_grouped_df = claims.calculate_sum_count(raw_invoices_df)
    invoices_grouped_df = calculate_claims_count(raw_invoices_df, invoices_grouped_df)
    invoices_grouped_df['grand total'] = calculate_grand_total(invoices_grouped_df)  
    return invoices_grouped_df


def filter_by_binnumber_authnumber(df, BIN_NUMBERS=None, CHAIN_CODES=None):
    if BIN_NUMBERS:
        df = df[(df.bin_number.isin(BIN_NUMBERS))]
    if CHAIN_CODES:
        df = df[(df.chain_code.isin(CHAIN_CODES))]
    df = df[~df.authorization_number.str.startswith('U')]
    
    return df

def define_transaction_type(df) -> pd.Series:    
    df['transaction_type'] = df.apply(lambda df_row: _build_transaction_type_string(df_row), axis=1)
    return df['transaction_type']

def _build_transaction_type_string(row):
    final_str = ''
    if row.basis_of_reimbursement_determination_resp == '04':
        final_str += 'unc'
    else:
        final_str += 'non_unc'
        if row.total_paid_response < 0:
            final_str += '_remunerative'
        else:
            final_str += '_non_remunerative'
    return final_str

def calculate_ingredient_cost_paid(transaction_type_column: pd.Series, ingredient_cost_paid_resp_column: pd.Series) -> pd.Series:    
    ingredient_cost_paid_resp_series = ingredient_cost_paid_resp_column.where(transaction_type_column != 'unc', 0)
    return ingredient_cost_paid_resp_series

def calculate_administration_fee_owed(transaction_type_column: pd.Series, total_paid_response_column: pd.Series) -> pd.Series:  
    administration_fee_owed_series = total_paid_response_column.where(transaction_type_column != 'unc', 0)
    return administration_fee_owed_series

def calculate_full_cost(df, quantity_column = 'quantity_dispensed', price_basis='awp') -> pd.Series:
    awp_column = (df[price_basis] *df[quantity_column]).apply(lambda x: Decimal(x).quantize(Decimal('0'), rounding=decimal.ROUND_HALF_UP)).divide(Decimal("100"))
    return awp_column

def add_fills_reversals_indicator(df, period_start, period_end, valid_from_column='valid_from', valid_to_column='valid_to'):
  fill_reversal_indicator_column = np.where((df[valid_from_column].dt.date >= period_start.date())&(df[valid_from_column].dt.date <= period_end.date())&(df[valid_to_column].dt.date > period_end.date()), Decimal(1),
                              np.where((df[valid_from_column].dt.date < period_start.date())&(df[valid_to_column].dt.date <= period_end.date())&(df[valid_to_column].dt.date >= period_start.date()), Decimal(-1), None))
  return fill_reversal_indicator_column

def apply_sign_for_fills_and_reversals(df, df_column):
  df_column = np.where(df['fill_reversal_indicator'] == 1, df_column, 
                np.where(df['fill_reversal_indicator'] == -1, - df_column, None))
  return df_column

def sum_and_group(df):
  df = df.groupby(['start_date', 'end_date', 'period_start', 'period_end', 'drug_type', 'basis_of_reimbursement_source', 'target_rate','days_supply_from','days_supply_to'], as_index=False).aggregate({'ingredient_cost_paid_resp':'sum','total_paid_response':'sum','awp':'sum','fill_reversal_indicator':'sum'}).rename(columns={'fill_reversal_indicator':'claim_count'})
  return df

def add_date_supply(df):
  if len(df.index) > 0:
    df['days_supply'] = df.apply(lambda df_row:
        'Any' if df_row.days_supply_from == 0.0 and df_row.days_supply_to == 99999.0 else
        f"{df_row.days_supply_from} +" if df_row.days_supply_to == 99999.0 else
        f"{df_row.days_supply_from}  -  {df_row.days_supply_to}", axis = 1)
  return df

def prepare_dataframe_for_excel_representation(df):
    df['bin+pcn+group'] = df['bin_number'].astype('object').fillna('') + '+' + df['process_control_number'].fillna('') + '+' + df['group_id'].fillna('')
    df['claim authorization number'] = '="' + df['authorization_number'].fillna('') + '"' # needed for excel to treat data as text not number
    df['product_id'] = df['product_id'].astype('str').str.zfill(11)
    df['ndc'] = '="' + df['product_id'].astype('string').fillna('') + '"' # needed for excel to treat data as text not number
    df['reversal date'] = df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    df['excluded claim indicator'] = 'N'
    df['exclusion reason'] = ''
    return df

def _calculate_effective_rate(row):
    if row['full_cost'] != Decimal('0'):
        return ((row['full_cost'] - row['ingredient_cost_paid_resp']) / row['full_cost']) * Decimal('100')
    else:
        return Decimal('0')

def calculate_effective_rate_and_dollar_variance(df, price_basis_column='price_basis', **kwargs):
    zero_full_cost_rows = df[df['full_cost'] == Decimal('0')][['authorization_number', 'drug_type', 'bin_number', 'partner']]
    count_zero_full_cost = len(zero_full_cost_rows)
    if count_zero_full_cost > 0:
        utils.send_message_with_dataframe_to_slack(kwargs.get('temp_slack_channel'), kwargs.get('slack_bot_token'), f"{count_zero_full_cost} Rows with zero full cost found - need further investigatioin.\n", dataframe=zero_full_cost_rows)

    df['actual effective rate'] = df.apply(_calculate_effective_rate, axis=1)

    # Quantize target effective rate
    df['target effective rate'] = df.target_rate.apply(lambda x: Decimal(x).quantize(Decimal('.001')))
    
    # Calculate effective rate variance
    df['effective rate variance'] = df['actual effective rate'] - df['target effective rate']
    
    # Calculate ingredient cost dollar variance
    df['ic dollar variance'] =  df['ingredient_cost_paid_resp'] - df['contracted_cost']
    # Calculate ingredient cost dollar variance
    df['df dollar variance'] = df['dispensing_fee_paid_resp'] - df['dfer_target_rate']
    
    # Set all values to zero for rows where price_basis is UNC
    unc_mask = df[price_basis_column] == 'UNC'
    df.loc[unc_mask, ['actual effective rate', 'target effective rate', 'effective rate variance', 'ic dollar variance']] = Decimal('0')

    df[['actual effective rate','target effective rate','effective rate variance','df dollar variance', 'ic dollar variance']] = df[['actual effective rate','target effective rate','effective rate variance','df dollar variance', 'ic dollar variance']].apply(lambda x: [utils.format_decimal(i) for i in x])
    return df[['actual effective rate', 'target effective rate', 'effective rate variance', 'ic dollar variance', 'df dollar variance']]

def define_reversal_indicator(df, period_start, period_end, valid_from_column='fill date'):
    df['fill_reversal_indicator'] = add_fills_reversals_indicator(df, period_start, period_end, valid_from_column=valid_from_column)
    df.loc[df['fill_reversal_indicator'] == 1, 'reversal indicator'] = df.transaction_code
    df.loc[df['fill_reversal_indicator'] == -1, 'reversal indicator'] = 'B2'
    return df['reversal indicator']

def adjust_math_fields(df):
    utils.cast_cents_to_dollars(df, column_names={'dispensing pharmacy u&c price':'usual_and_customary_charge','taxes paid':'percentage_sales_tax_amount_paid',
    'administration fee':'total_paid_response','total amount paid':'total_paid_response','patient amount':'patient_pay_resp','ingredient cost paid':'ingredient_cost_paid_resp',
    'dispensing fee paid':'dispensing_fee_paid_resp'})
    df['administration fee'] = - df['administration fee']
    df[['taxes paid','total amount paid','ingredient cost paid','administration fee']] = df[['taxes paid','total amount paid','ingredient cost paid','administration fee']].applymap(lambda x: Decimal('0') if x == -0 else x)   
    return df

def group_and_calculate_metrics(df):
    df = df.groupby(['period_start', 'period_end', 'year', 'transaction_type', 'drug_type'], as_index=False).aggregate({'ingredient_cost_paid_resp':'sum','total_paid_response':'sum','fill_reversal_indicator':'sum'}).rename(columns={'fill_reversal_indicator':'claim_count'})
    df['basis_of_reimbursement_source'] = 'AWP'
    return df

def calculate_sum_count(df):
    df = df.groupby(['period_start', 'period_end', 'invoice_date', 'due_date', 'net', 'invoice_number', 'drug_type', 'basis_of_reimbursement_source'], as_index=False).aggregate({'ingredient_cost_paid_resp':'sum', 'total_paid_response':'sum','claim_count':'sum'}).rename(columns={'claim_count':'claims count'})
    return df

def calculate_claims_count(raw_cvs_invoices_df, cvs_invoices_grouped_df):
    for price_source in raw_cvs_invoices_df.basis_of_reimbursement_source.unique():
        # calculate claims count for each combinaction of transaction type & drug type; need it to calculate 'total paid claims'/'total remunerative paid claims'/'claims count'
        claims_per_transaction_type = raw_cvs_invoices_df.loc[raw_cvs_invoices_df.basis_of_reimbursement_source == price_source].groupby(['transaction_type','drug_type'],as_index=False)[['claim_count']].sum().rename(columns={'claim_count':'claims count'})
        for drug_type in ['brand','generic']:
            cvs_invoices_grouped_df.loc[(cvs_invoices_grouped_df.drug_type == drug_type) & (cvs_invoices_grouped_df.basis_of_reimbursement_source == price_source), 'total paid claims'] = claims_per_transaction_type[(claims_per_transaction_type.drug_type == drug_type)&(claims_per_transaction_type.transaction_type.isin(['non_unc_non_remunerative','non_unc_remunerative']))]['claims count'].sum()
            cvs_invoices_grouped_df.loc[(cvs_invoices_grouped_df.drug_type == drug_type) & (cvs_invoices_grouped_df.basis_of_reimbursement_source == price_source), 'total remunerative paid claims'] = claims_per_transaction_type[(claims_per_transaction_type.drug_type == drug_type)&(claims_per_transaction_type.transaction_type.isin(['non_unc_remunerative']))]['claims count'].sum()
    return cvs_invoices_grouped_df

def calculate_grand_total(cvs_invoices_grouped_df) -> pd.Series:  
    cvs_invoices_grouped_grand_total = cvs_invoices_grouped_df['total_paid_response'].sum()
    return cvs_invoices_grouped_grand_total

def apply_number_formatting(df):
    df['total quantity dispensed'] = df['total quantity dispensed'].apply(lambda x: Decimal(x).quantize(Decimal('0.01')))
    df[['dispensing pharmacy u&c price','taxes paid','administration fee','total amount paid','patient amount','average wholesale price (awp)','ingredient cost paid','dispensing fee paid']] = \
        df[['dispensing pharmacy u&c price','taxes paid','administration fee','total amount paid','patient amount','average wholesale price (awp)','ingredient cost paid','dispensing fee paid']].applymap(lambda x: Decimal(x).quantize(Decimal('0.0001')))
    return df
    
def determine_basis_of_rembursment_abbreviation(basis_of_reimbursment_resp_column: pd.Series) -> pd.Series:
    mapping = {
        '03': 'AWP',
        '07': 'MAC',
        '20': 'NADAC',
        '04': 'UNC',
        '13': 'WAC'
    }
    return basis_of_reimbursment_resp_column.map(mapping)

def set_precision_for_numeric_columns(df):
    if 'quantity_dispensed' in df.columns:
        df['quantity_dispensed'] = df['quantity_dispensed'].apply(lambda x: Decimal(x).quantize(Decimal('0.01')))
    
    columns_to_format = [
        'usual_and_customary_charge',
        'percentage_sales_tax_amount_paid',
        'total_paid_response',
        'patient_pay_resp',
        'average wholesale price (awp)',
        'ingredient_cost_paid_resp',
        'dispensing_fee_paid_resp'
    ]
    
    for column in columns_to_format:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: Decimal(x).quantize(Decimal('0.01')))
    
    return df

def calculate_contracted_cost(df, contracted_ger, contracted_ber, quantity_column='quantity_dispensed', basis='awp') -> pd.Series:
    contracted_ger = 1 - Decimal(contracted_ger)
    contracted_ber = 1 - Decimal(contracted_ber)
    df = cast_existing_columns_to_decimal(df, column_names=['awp', 'mac', 'wac', 'nadac', quantity_column], fillna_flag=True)
    awp_column = pd.Series(index=df.index, dtype='object')
    
    for idx, row in df.iterrows():
        if row['generic indicator (multi-source indicator)'].lower() in ['g', 'generic']:
            contracted_price = row[basis] * row[quantity_column] * (contracted_ger if contracted_ger != Decimal(0) else Decimal(1))
        elif row['generic indicator (multi-source indicator)'].lower() in ['b', 'brand']:
            contracted_price = row[basis] * row[quantity_column] * (contracted_ber if contracted_ber != Decimal(0) else Decimal(1))
        else:
            raise Exception(f"multi-source indicator value different from 'B/brand' or 'G/generic' is not allowed. Current value: {row['generic indicator (multi-source indicator)']}")
        
        awp_column[idx] = Decimal(contracted_price).quantize(Decimal('0'), rounding=decimal.ROUND_HALF_UP) / Decimal('100')
        awp_column = awp_column.apply(Decimal)

    return awp_column

def cast_existing_columns_to_decimal(df, column_names, fillna_flag=True):
    for column in column_names:
        if column in df.columns:
            if fillna_flag:
                df[column] = df[column].fillna(0).apply(lambda x: Decimal(str(x)))
            else:
                df[column] = df[column].apply(lambda x: Decimal(str(x)) if pd.notna(x) else x)
    return df

def cast_existing_columns_to_dollars(df, column_names):
    for column in column_names:
        if column in df.columns:
            df[column] = df[column] / Decimal('100')
    return df

def calculate_contracted_and_full_cost(df, quantity_column='quantity_dispensed', contracted_rate_column='target_rate', basis_of_reumbursement_str_abbreviation_column='reconciliation_price_basis') -> pd.DataFrame:

    # Convert contracted_rate to Decimal (e.g. contracted_rate for cvs = 71.5; 1 - (71.5/100) = 0.285; if it's )
    df['contracted_multiplier'] = 1 - df[contracted_rate_column].apply(Decimal).divide(Decimal('100'))
    df = utils.cast_columns_to_decimal(df, column_names=['usual_and_customary_charge', quantity_column, 'awp', 'mac', 'nadac', 'gpi_nadac', 'wac'], fillna_flag=True)
    # Define a mapping from price_basis to the respective column names
    price_basis_to_column = {
        'AWP': 'awp',
        'MAC': 'mac',
        'WAC': 'wac',
        'NADAC': 'nadac',
        'UNC': 'unc'
    }

    # Initialize the Series for storing contracted and full prices
    contracted_prices = pd.Series(index=df.index, dtype='object')
    full_prices = pd.Series(index=df.index, dtype='object')

    for idx, row in df.iterrows():
        # Get the column to process based on price_basis
        column_to_process = price_basis_to_column.get(row[basis_of_reumbursement_str_abbreviation_column])

        if column_to_process is None:
            raise Exception(f"Invalid price_basis value: {row[basis_of_reumbursement_str_abbreviation_column]}")

        # handle the NADAC case where we need to fallback to 'gpi_nadac' if 'nadac' is null
        if row[basis_of_reumbursement_str_abbreviation_column] == 'NADAC':
            column_to_process = 'gpi_nadac' if pd.isna(row['nadac']) or row['nadac'] == 0 else 'nadac'

        # Calculate the contracted price
        if column_to_process != 'unc':
            contracted_price = row[column_to_process] * row[quantity_column] * row['contracted_multiplier']
        else:
            contracted_price = row['usual_and_customary_charge']

        # Calculate the full price
        if column_to_process != 'unc':
            full_price = row[column_to_process] * row[quantity_column] 
        else:
            full_price = row['usual_and_customary_charge']

        # Round the results and store them in the respective Series
        contracted_prices[idx] = Decimal(contracted_price).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
        full_prices[idx] = Decimal(full_price).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

    # Return a DataFrame with both columns
    return pd.DataFrame({
        'contracted_cost': contracted_prices,
        'full_cost': full_prices
    })
