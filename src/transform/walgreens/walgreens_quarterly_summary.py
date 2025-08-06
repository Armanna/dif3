from transform import utils, claims, pandas_helper
from sources import pharmacies
from decimal import Decimal

def transform_walgreens_summary_data(raw_claims_df, raw_pharmacy_df, raw_ndc_v2_df, raw_chain_rates, raw_ndc_costs_v2_df, period_start, period_end):
    walgreens = pharmacies.Walgreens()
    raw_claims_df = claims.filter_by_binnumber_authnumber(raw_claims_df, walgreens.BIN_NUMBERS)
    result_df = pandas_helper.left_join_with_condition(raw_claims_df, raw_pharmacy_df[['id','chain_code','state_abbreviation','valid_from','valid_to']], left_on='npi', right_on='id')
    result_df = pandas_helper.left_join_with_condition(result_df, raw_ndc_v2_df[['id','is_otc','multi_source_code','valid_from','valid_to']], left_on='product_id', right_on='id').drop(columns=['id_y','id','id_x'])
    result_df['drug type'] = walgreens.add_brand_generic_indicator(result_df)
    result_df = pandas_helper.left_join_with_condition(result_df, raw_chain_rates[(raw_chain_rates['chain_name']=='walgreens') & (raw_chain_rates['partner'] == '') & (raw_chain_rates['state_abbreviation'] != '')][['drug_type','bin_number','state_abbreviation','target_rate','valid_from','valid_to']], left_on=['drug type','bin_number','state_abbreviation'], right_on=['drug_type','bin_number','state_abbreviation'], filter_by='valid_from_x') \
        .rename(columns={'target_rate':'per_state_target_rate'})
    result_df = pandas_helper.left_join_with_condition(result_df, raw_chain_rates[(raw_chain_rates['chain_name']=='walgreens') & (raw_chain_rates['partner'] == '') & (raw_chain_rates['state_abbreviation'] == '')][['target_rate','drug_type','bin_number','state_abbreviation','valid_from','valid_to']], left_on=['drug type','bin_number'], right_on=['drug_type','bin_number'], filter_by='valid_from_x') \
        .rename(columns={'target_rate':'default_target_rate'})
    summary_df = pandas_helper.left_join_with_condition(result_df, raw_ndc_costs_v2_df[['awp','ndc','valid_from','valid_to']], left_on='product_id', right_on='ndc').drop(columns=['ndc'])    
    summary_df['period_start'] = period_start
    summary_df['period_end'] = period_end
    summary_df['year'] = period_start.year
    summary_df['basis_of_reimbursement_source'] = 'AWP'
    summary_df['awp'] = (summary_df.awp * summary_df['quantity_dispensed']).apply(lambda x: Decimal(x)).divide(Decimal("100"))
    summary_df['target_rate'] = summary_df['per_state_target_rate'].combine_first(summary_df['default_target_rate']) # if state_abbreviation in the list of (MA,AK,HI,AS,FM,GU,MH,MP,PR,PW,VI) - use respective target rate, in all other cases - use default
    summary_df = summary_df.drop(['per_state_target_rate', 'default_target_rate'], axis=1)
    invoice_grouped_df = summary_df.groupby(['period_start', 'period_end', 'year', 'drug type','basis_of_reimbursement_source','target_rate'], as_index=False).aggregate({'year':'count', 'ingredient_cost_paid_resp':'sum', 'total_paid_response':'sum', 'awp':'sum'}).rename(columns={'year':'claim count'}).sort_values(by='claim count', ascending=False)
    invoice_grouped_df['period_start'],invoice_grouped_df['period_end'] = period_start.strftime('%m/%d/%Y'), period_end.strftime('%m/%d/%Y')
    for column in ['ingredient_cost_paid_resp','total_paid_response']:
        invoice_grouped_df[column] = utils.cast_cents_to_dollars(invoice_grouped_df, column, add_fill_reversal_sign=False)
    invoice_grouped_df[['actual effective rate','target effective rate','effective rate variance','dollar variance']] = claims.calculate_effective_rate_and_dollar_variance(invoice_grouped_df)
    invoice_grouped_df = _select_columns_andapply_formatting(invoice_grouped_df)
    return invoice_grouped_df

def _select_columns_andapply_formatting(df):
    df = df[['period_start', 'period_end', 'drug type', 'target effective rate', 'basis_of_reimbursement_source', 'claim count', 'ingredient_cost_paid_resp', 'total_paid_response', 'awp', 'actual effective rate','effective rate variance', 'dollar variance']].rename(columns={'ingredient_cost_paid_resp':'total ingredient cost', 'total_paid_response':'total administration fee', 'awp':'total awp'})
    df['total administration fee'] = - df['total administration fee']
    utils.apply_formatting(df, columns=['total ingredient cost', 'total administration fee'], formatting='{:,.2f}')
    return df
