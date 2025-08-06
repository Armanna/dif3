import pandas as pd
import datetime
from decimal import Decimal

from transform import pandas_helper
from sources import pharmacies

chain_class = pharmacies.Walmart

def join_ndcs_sources_for_specific_date(claims_df, source_dict):
    result_df = claims_df.copy()

    original_dates_df = claims_df[['authorization_number', 'claim_date_of_service']].copy() # store the original claim_date_of_service and authorization_number values as a reference dataframe
    fixed_date = pd.Timestamp(datetime.datetime(2024, 1, 3)) # temporarily set claim_date_of_service to a fixed date for specific merges
    result_df['claim_date_of_service'] = fixed_date

    # merge only 'nadac' and 'gpi_nadac'columns from 'ndc_costs_v2' as of January 3rd 2024
    if 'ndc_costs_v2' in source_dict and not source_dict['ndc_costs_v2'].empty:
        result_df = pandas_helper.left_join_with_condition(
            result_df,
            source_dict['ndc_costs_v2'][['nadac', 'gpi_nadac', 'valid_from', 'valid_to', 'ndc']],
            left_on='product_id', right_on='ndc'
        ).drop(columns=['ndc'])
    
    # merge only 'nadac_is_generic' column from 'ndcs_v2' as of January 3rd 2024
    if 'ndcs_v2' in source_dict and not source_dict['ndcs_v2'].empty:
        result_df = pandas_helper.left_join_with_condition(
            result_df,
            source_dict['ndcs_v2'][['id', 'valid_from', 'valid_to', 'nadac_is_generic']],
            left_on='product_id', right_on='id'
        )

    # restore the original claim_date_of_service based on authorization_number
    result_df = result_df.merge(
        original_dates_df, 
        on='authorization_number', 
        suffixes=('', '_original'),
        how='left'
    )

    result_df['claim_date_of_service'] = result_df['claim_date_of_service_original'] # replace the fixed date with the original date from 'claim_date_of_service_original'
    result_df = result_df.drop(columns=['claim_date_of_service_original']) # drop the extra column used for restoring

    # perform the final merges with the real claim_date_of_service for the rest of the columns
    if 'ndc_costs_v2' in source_dict and not source_dict['ndc_costs_v2'].empty:
        result_df = pandas_helper.left_join_with_condition(
            result_df,
            source_dict['ndc_costs_v2'][['awp', 'wac', 'ndc', 'valid_from', 'valid_to']],
            left_on='product_id', right_on='ndc'
        ).drop(columns=['ndc'])

    if 'ndcs_v2' in source_dict and not source_dict['ndcs_v2'].empty:
        result_df = pandas_helper.left_join_with_condition(
            result_df,
            source_dict['ndcs_v2'][['id', 'is_otc', 'multi_source_code', 'valid_from', 'valid_to', 'name_type_code']],
            left_on='product_id', right_on='id'
        )

    result_df = result_df.reset_index(drop=True)
    return result_df.drop_duplicates().reset_index(drop=True)

def join_excluded_ndcs(claims_df, excluded_ndcs):
    excluded_ndcs = excluded_ndcs.drop(columns=['chain_name'])
    claims_df = pandas_helper.left_join_with_condition(
            claims_df,
            excluded_ndcs,
            left_on='product_id', right_on='ndc'
        ).drop(columns=['ndc'])
    claims_df['reason'] = claims_df['reason'].str.replace('walmart_', '')
    return claims_df.drop_duplicates().reset_index(drop=True)

def join_four_dollar_ndcs(claims_df, ndcs):
    ndcs = ndcs.drop(columns=['drug_type'])
    claims_df = pandas_helper.left_join_with_condition(
        claims_df,
        ndcs,
        left_on='product_id', right_on='ndc'
    ).rename(columns={'ndc': 'four_dol_ndc_flag'})
    claims_df['four_dol_ndc_flag'] = claims_df['four_dol_ndc_flag'].notna()
    return claims_df.drop_duplicates().reset_index(drop=True)
    
def _calculate_claim_indicator(days_supply):
    if days_supply >= 1 and days_supply <= 30:
        return '1-30'
    elif days_supply > 30 and days_supply <= 90:
        return '31-90'
    else:
        return '90+'

def add_specific_walmart_columns(df):
    for name in ['Contracted Vaccine Administration Fee', 'Total Cost Share Paid', 'Vaccine Administration Fee Paid']:
        df[name] = ''
    df['Plan Category'] = 'Discount Card'
    df['U&C Indicator'] = df['price_basis'].apply(lambda x: 'True' if x == 'UNC' else 'False')
    df['Actual Rate'] = (df['ingredient_cost_paid_resp'] / df['full_cost'] - 1).apply(lambda x: x.quantize(Decimal('.01')))
    df['Variant Amount'] = (df['ingredient_cost_paid_resp'] - (df['full_cost'] * (1 - df['target_rate'] / Decimal('100')))) + (df['dispensing_fee_paid_resp'] - df['dfer_target_rate'] * df['fill_reversal_indicator'])#.apply(lambda x: x.quantize(Decimal('1')))
    df['30/90 Day Claim Indicator'] = df['days_supply'].apply(_calculate_claim_indicator)
    df['Unique Drug Indicator'] = df['Exclusion reason']
    df['Network'] = 'HIPPO Discount Card Network'
    return df

def build_excluded_items_dataframe(df):
    df = df[df['Exclusion reason'] != ''][['Exclusion reason', 'Plan Category', 'usual_and_customary_charge', 'patient_pay_resp', 'percentage_sales_tax_amount_paid','fill_reversal_indicator']]
    df_grouped = df.groupby(['Exclusion reason', 'Plan Category'], as_index=False).agg({
        'usual_and_customary_charge': 'sum',
        'percentage_sales_tax_amount_paid': 'sum',
        'patient_pay_resp': 'sum',
        'fill_reversal_indicator': 'sum',
    }).rename(columns={'fill_reversal_indicator': 'Claim Count'})

    df_grouped['Amt Under U&C'] = df_grouped['usual_and_customary_charge'] - (df_grouped['patient_pay_resp'] - df_grouped['percentage_sales_tax_amount_paid'])
    df_grouped = df_grouped.rename(columns={'Exclusion reason': 'Exclusion Category'})
    df_final = df_grouped[['Exclusion Category', 'Plan Category', 'Claim Count', 'Amt Under U&C']]

    total_row = pd.DataFrame({
        'Exclusion Category': ['Total'],
        'Plan Category': [''],  # Empty for Plan Category in the total row
        'Claim Count': [df_final['Claim Count'].sum()],
        'Amt Under U&C': [df_final['Amt Under U&C'].sum()]
    })

    df_final = pd.concat([df_final, total_row], ignore_index=True)
    return df_final

def admin_fee_report_dataframe(df):
    df = df[df['Excluded Claim Indicator'] == 'N']
    df['Contracted Admin'] = Decimal(chain_class.ADMIN_FEE).quantize(Decimal('0.00001'))

    df_grouped = df.groupby(['Network', 'Contracted Admin'], as_index=False).agg({
        'total_paid_response': 'sum',
        'fill_reversal_indicator': 'sum',
    }).rename(columns={'fill_reversal_indicator': 'Rx Count'})

    df_grouped['Rx Count'] = df_grouped['Rx Count'].apply(lambda x: Decimal(x))
    df_grouped['total_paid_response'] = df_grouped['total_paid_response'].apply(lambda x: Decimal(x))
    df_grouped['Admin Fee /Rx'] = (df_grouped['total_paid_response'] / df_grouped['Rx Count']).apply(lambda x: x.quantize(Decimal('0.000001')))
    df_grouped['Delta'] = (df_grouped['Admin Fee /Rx'] - df_grouped['Contracted Admin']).apply(lambda x: x.quantize(Decimal('0.000001')))
    df_grouped['Variance'] = (df_grouped['Delta'] * df_grouped['Rx Count']).apply(lambda x: x.quantize(Decimal('0.000001')))
    df_grouped = df_grouped.rename(columns={'total_paid_response': 'Admin Fee Paid'})
    df_grouped['Admin Fee Paid'] = df_grouped['Admin Fee Paid'].apply(lambda x: x.quantize(Decimal('0.001')))
    df_final = df_grouped[['Network', 'Contracted Admin', 'Rx Count', 'Admin Fee Paid', 'Admin Fee /Rx', 'Delta', 'Variance']]

    total_row = pd.DataFrame({
        'Network': ['Total'],
        'Contracted Admin': [''],
        'Rx Count': [df_final['Rx Count'].sum()],
        'Admin Fee Paid': [df_final['Admin Fee Paid'].sum()],
        'Admin Fee /Rx': [''],
        'Delta': [''],
        'Variance': [df_final['Variance'].sum()]
    })

    df_final = pd.concat([df_final, total_row], ignore_index=True)
    return df_final

def build_walmart_quarterly_summary(df, process_reversals_flag=True):
    if process_reversals_flag:
        mask = df['Excluded Claim Indicator'] == 'N'
    else:
        mask = (df['Excluded Claim Indicator'] == 'N') & (df['reversal indicator'] != 'B2')

    df = df[mask]
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby(['Network', 'drug_type', 'days_supply_group', 'reconciliation_price_basis', 'target_rate', 'dfer_target_rate'], as_index=False).agg({
        'fill_reversal_indicator': 'sum',
        'full_cost': 'sum',
        'ingredient_cost_paid_resp': 'sum',
        'dispensing_fee_paid_resp': 'sum',
        'contracted_cost': 'sum',
    }).rename(columns={'fill_reversal_indicator': 'Claim Count'})

    grouped['Actual Rate'] = ((1 - grouped['ingredient_cost_paid_resp'] / grouped['full_cost']) * Decimal('100')).apply(lambda x: x.quantize(Decimal('.01')))
    grouped['Actual Fee'] = (grouped['dispensing_fee_paid_resp'] / grouped['Claim Count']).apply(lambda x: x.quantize(Decimal('.01')))
    grouped['Rate Variance'] = (grouped['ingredient_cost_paid_resp'] - (grouped['full_cost'] * (1 - grouped['target_rate'] / Decimal('100')))).apply(lambda x: x.quantize(Decimal('.01')))
    grouped['DF Variance'] = (grouped['dispensing_fee_paid_resp'] - (grouped['dfer_target_rate'] * grouped['Claim Count'])).apply(lambda x: x.quantize(Decimal('.01')))
    grouped['Total Variance'] = (grouped['DF Variance'] + grouped['Rate Variance']).apply(lambda x: x.quantize(Decimal('.01')))
    grouped = grouped.rename(columns={'drug_type':'Brand/Generic', 'days_supply_group': 'Day Supply', 'reconciliation_price_basis': 'Reimbursement Type', 'target_rate': 'Contract Rate', 'dfer_target_rate': 'Contracted Dispensig Fee',  'full_cost': 'Full Cost', 'ingredient_cost_paid_resp': 'Ingredient Cost Paid', 'dispensing_fee_paid_resp': 'Dispensing Fee Paid'})

    total_row = pd.DataFrame({
        'Network': ['Total'],
        'Brand/Generic': [''],
        'Day Supply': [''],
        'Reimbursement Type': [''],
        'Contract Rate': [''],
        'Contracted Dispensig Fee': [''],
        'Claim Count': [grouped['Claim Count'].sum()],
        'Full Cost': [grouped['Full Cost'].sum()],
        'Ingredient Cost Paid': [grouped['Ingredient Cost Paid'].sum()],
        'Dispensing Fee Paid': [grouped['Dispensing Fee Paid'].sum()],
        'Actual Rate': [''],
        'Actual Fee': [''],
        'Rate Variance': [grouped['Rate Variance'].sum()],
        'DF Variance': [grouped['DF Variance'].sum()],
        'Total Variance': [grouped['Total Variance'].sum()]
    })

    grouped = pd.concat([grouped, total_row], ignore_index=True)
    return grouped[['Network', 'Brand/Generic', 'Day Supply', 'Reimbursement Type', 'Contract Rate', 'Contracted Dispensig Fee', 'Claim Count', 'Full Cost', 'Ingredient Cost Paid', 'Dispensing Fee Paid', 'Actual Rate', 'Actual Fee', 'Rate Variance', 'DF Variance', 'Total Variance']]
