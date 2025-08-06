import numpy as np
import pandas as pd
from decimal import Decimal
from sources import pharmacies
from sources import claims as sources_claims
from transform import claims, utils

chain = pharmacies.KrogerAndHarrisTeeter

def transform_transactions_data(raw_claims_df, source_dict, period_start, period_end):
    transactions_df = sources_claims.join_default_source_dataframes(raw_claims_df, source_dict)
    transactions_df = utils.cast_cents_to_dollars(transactions_df, column_names=['percentage_sales_tax_amount_paid', 'usual_and_customary_charge', 'total_paid_response', 'ingredient_cost_paid_resp', 'dispensing_fee_paid_resp', 'patient_pay_resp'], add_fill_reversal_sign=False)
    transactions_df['claim_type'] = chain.determine_dea_claim_type(transactions_df['dea_class_code'])
    transactions_df['reversal indicator'] = claims.define_reversal_indicator(transactions_df, period_start, period_end, valid_from_column='valid_from')
    for col_name, pd_series in {'ingredient_cost_paid_resp':transactions_df['ingredient_cost_paid_resp'], 'total_paid_response': -transactions_df['total_paid_response'], 'patient_pay_resp': transactions_df['patient_pay_resp']}.items():
        transactions_df[col_name] = claims.apply_sign_for_fills_and_reversals(transactions_df, pd_series)
    transactions_df['generic indicator (multi-source indicator)'] = chain.add_brand_generic_indicator(transactions_df['multi_source_code'])
    transactions_df['fill_type'] = claims.determine_basis_of_rembursment_abbreviation(transactions_df['basis_of_reimbursement_determination_resp'])
    transactions_df['Basis of reimbursement determination'] = transactions_df['fill_type']
    transactions_df['contracted_disp_fee'] = transactions_df.apply(calculate_contracted_disp_fee, axis=1)
    transactions_df['contracted_awp'] = claims.calculate_contracted_cost(transactions_df, Decimal(chain.TARGET_GER), Decimal(chain.TARGET_BER), basis='awp')
    transactions_df['contracted_nadactr'] = claims.calculate_contracted_cost(transactions_df, Decimal('0'), Decimal('0'), basis='nadac')
    transactions_df['contracted_mac'] = claims.calculate_contracted_cost(transactions_df, Decimal('0'), Decimal('0'), basis='mac')
    transactions_df['awp'] = claims.calculate_full_cost(transactions_df, price_basis='awp')
    transactions_df['nadac'] = claims.calculate_full_cost(transactions_df, price_basis='nadac')
    transactions_df['mac'] = claims.calculate_full_cost(transactions_df, price_basis='mac')
    transactions_df = claims.set_precision_for_numeric_columns(transactions_df)
    transactions_df['Total Pharmacy reimbursement'] = transactions_df['patient_pay_resp'] - transactions_df['total_paid_response']
    transactions_df['Unique Identifier'] = transactions_df['bin_number'].astype(str) + transactions_df['process_control_number'].astype(str) + transactions_df['network_reimbursement_id'].astype(str)
    transactions_df[['Excluded Claim Indicator', 'Exclusion reason']] = transactions_df.apply(chain.set_exclusion_flags, axis=1, result_type='expand')
    transactions_df = claims.prepare_dataframe_for_excel_representation(transactions_df).rename(columns=chain.COLUMN_RENAME_DICT) # modify dataframe to fit transaction file requirements
    transactions_df[['Amount due to Vendor', 'Amount due to Pharmacy']] = transactions_df.apply(_calculate_amounts, axis=1, result_type='expand')
    transactions_df['COB indicator'] = ''
    transactions_df['Reversal date'] = np.where(transactions_df['reversal indicator'] != 'B2', '', transactions_df['reversal date'])
    transactions_df[['Fill date', 'Date of Service']] = transactions_df[['Fill date', 'Date of Service']].applymap(lambda x: x.strftime('%Y-%m-%d'))
    return transactions_df

def _calculate_amounts(row):
    """
    This function helps to define wether we owe pharmacy money or pharmacy owe us.
    Logic: 
        if it's fill (not reversal) and:
            Admin Fee value is positive - pharmacy owe us
            Admin Fee value is negative - we owe pharmacy
        if it's reversal and:
            Admin Fee value is positive - we owe pharmacy
            Admin Fee value is negative - pharmacy owe us
        if Admin Fee is zero - nobody owe anything 
    """
    if float(row['Administration Fee']) > 0 and row['reversal indicator'] == "B1":
        return row['Administration Fee'], 0
    elif float(row['Administration Fee']) < 0 and row['reversal indicator'] == "B1":
        return 0, row['Administration Fee']
    elif float(row['Administration Fee']) > 0 and row['reversal indicator'] == "B2":
        return 0, row['Administration Fee']
    elif float(row['Administration Fee']) < 0 and row['reversal indicator'] == "B2":
        return row['Administration Fee'], 0
    else:
        return 0, 0

def calculate_contracted_disp_fee(row):
    if row['fill_type'] == 'AWP': # dispensig fee values for AWP claims
        fee_dict = chain.DISP_FEE_DICT['AWP']
    else:  # dispensig fee values for NADAC claims
        fee_dict = chain.DISP_FEE_DICT[row['claim_type']]
    
    if row['generic indicator (multi-source indicator)'].lower() in ['b', 'brand']:
        drug_type = 'brand'
    elif row['generic indicator (multi-source indicator)'].lower() in ['g', 'generic']:
        drug_type = 'generic'
    else:
        raise Exception(f'Unexpected Brand/Generic indicator. The value in "generic indicator (multi-source indicator)" columns is different from "Brand - B" or "Generic - G". Current value: {row["generic indicator (multi-source indicator)"]}')
    
    days_supply_key = '1-83' if row['days_supply'] <= 83 else '84+'
    
    return Decimal(fee_dict[drug_type][days_supply_key])
