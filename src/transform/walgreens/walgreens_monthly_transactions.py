from datetime import timedelta
from decimal import Decimal
from sources import pharmacies
from sources import claims as sources_claims
from transform import utils, claims
from transform.walgreens import walgreens_utils, walgreens_claims


def transform_walgreens_transactions_data(raw_claims_df, source_dict):
    walgreens = pharmacies.Walgreens()
    raw_transactions_df = sources_claims.join_default_source_dataframes(raw_claims_df, source_dict)
    transactions_df = claims.filter_by_binnumber_authnumber(raw_transactions_df, walgreens.BIN_NUMBERS)
    transactions_df = claims.prepare_dataframe_for_excel_representation(transactions_df).rename(columns={'bin_number':'unique identifier','prescription_reference_number':'rx#','claim_date_of_service':'service date','valid_from':'fill date','fill_number':'refill number','drug_name':'drug name','quantity_dispensed':'total quantity dispensed','days_supply':'total days supply','basis_of_reimbursement_determination_resp':'basis of reimbursement determination'}) # modify dataframe to fit transaction file requirements 
    transactions_df['generic indicator (multi-source indicator)'] = walgreens.add_brand_generic_indicator(transactions_df)
    transactions_df = walgreens_claims.add_walgreens_reversal_indicator(transactions_df)
    transactions_df = _adjust_math_fields(transactions_df)
    transactions_df['average wholesale price (awp)'] = claims.calculate_awp(transactions_df, quantity_column='total quantity dispensed')
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
    transactions_final_df['reversal date'] = ''
    transactions_final_df[['fill date', 'service date']] = transactions_final_df[['fill date', 'service date']].applymap(lambda x: x.strftime('%Y-%m-%d'))
    return transactions_final_df

def _adjust_math_fields(df):
    utils.cast_cents_to_dollars(df, column_names={'dispensing pharmacy u&c price':'usual_and_customary_charge','taxes paid':'percentage_sales_tax_amount_paid',
    'administration fee':'total_paid_response','total amount paid':'total_paid_response','patient amount':'patient_pay_resp','ingredient cost paid':'ingredient_cost_paid_resp',
    'dispensing fee paid':'dispensing_fee_paid_resp'}, add_fill_reversal_sign = False)
    df['administration fee'] = - df['administration fee']
    df[['taxes paid','total amount paid','ingredient cost paid','administration fee']] = df[['taxes paid','total amount paid','ingredient cost paid','administration fee']].applymap(lambda x: Decimal('0') if x == -0 else x)   
    return df
