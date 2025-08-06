import numpy as np
from sources import pharmacies
from sources import claims as sources_claims
from transform import claims


def transform_cvs_transactions_data(raw_claims_df, source_dict, period_start, period_end):
    cvs = pharmacies.CVS()
    raw_transactions_df = sources_claims.join_default_source_dataframes(raw_claims_df, source_dict)
    transactions_df = claims.filter_by_binnumber_authnumber(raw_transactions_df, cvs.BIN_NUMBERS)
    transactions_df = claims.prepare_dataframe_for_excel_representation(transactions_df).rename(columns={'bin_number':'unique identifier','prescription_reference_number':'rx#','claim_date_of_service':'service date','valid_from':'fill date','fill_number':'refill number','drug_name':'drug name','quantity_dispensed':'total quantity dispensed','days_supply':'total days supply','basis_of_reimbursement_determination_resp':'basis of reimbursement determination'}) # modify dataframe to fit transaction file requirements 
    transactions_df['reversal indicator'] = claims.define_reversal_indicator(transactions_df, period_start, period_end)
    transactions_df['generic indicator (multi-source indicator)'] = cvs.add_brand_generic_indicator_invoice(transactions_df)
    transactions_df = claims.adjust_math_fields(transactions_df)
    transactions_df['average wholesale price (awp)'] = claims.calculate_awp(transactions_df, quantity_column = 'total quantity dispensed')
    transactions_df = claims.apply_number_formatting(transactions_df)
    transactions_final_df = _select_fields_for_exporting(transactions_df)
    return transactions_final_df

def _select_fields_for_exporting(transactions_df):
    transactions_final_df = transactions_df[['bin+pcn+group', 'unique identifier', 'npi',
        'claim authorization number', 'rx#', 'service date', 'fill date',
        'reversal date', 'refill number', 'ndc', 'drug name',
        'generic indicator (multi-source indicator)',
        'total quantity dispensed', 'total days supply',
        'dispensing pharmacy u&c price', 'taxes paid', 'administration fee',
        'total amount paid', 'patient amount', 'average wholesale price (awp)',
        'ingredient cost paid', 'dispensing fee paid',
        'excluded claim indicator', 'exclusion reason', 'reversal indicator',
        'basis of reimbursement determination']]
    transactions_final_df['reversal date'] = np.where(transactions_final_df['reversal indicator'] != 'B2', '', transactions_final_df['reversal date'])
    transactions_final_df[['fill date', 'service date']] = transactions_final_df[['fill date', 'service date']].applymap(lambda x: x.strftime('%Y-%m-%d'))
    return transactions_final_df
