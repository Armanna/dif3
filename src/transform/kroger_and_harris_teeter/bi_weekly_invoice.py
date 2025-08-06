import pandas as pd
from transform import utils, claims, invoices
from sources import claims as sources_claims
from sources import pharmacies

chain = pharmacies.KrogerAndHarrisTeeter

def transform_invoice_data(raw_claims_df, raw_ndchist_df, raw_mf2ndc_df, period_start, period_end):
    raw_invoice_claims_df = sources_claims.join_ndcs_data(raw_claims_df, raw_ndchist_df)
    invoice_claims_df = pd.merge(raw_invoice_claims_df, raw_mf2ndc_df[['ndc_upc_hri']], how='left', left_on='product_id', right_on='ndc_upc_hri')
    invoice_claims_df['period_start'] = period_start
    invoice_claims_df['period_end'] = period_end
    invoice_claims_df['year'] = period_start.year
    invoice_claims_df['basis_of_reimbursement_source'] = claims.determine_basis_of_rembursment_abbreviation(invoice_claims_df['basis_of_reimbursement_determination_resp'])
    invoice_claims_df['transaction_type'] = claims.define_transaction_type(invoice_claims_df)
    invoice_claims_df['drug_type'] = chain.add_brand_generic_indicator(invoice_claims_df['multi_source_code'])
    invoice_claims_df['fill_reversal_indicator'] = claims.add_fills_reversals_indicator(invoice_claims_df, period_start, period_end)
    for col_name, pd_series in {'ingredient_cost_paid_resp':invoice_claims_df['ingredient_cost_paid_resp'], 'total_paid_response': -invoice_claims_df['total_paid_response']}.items():
        invoice_claims_df[col_name] = claims.apply_sign_for_fills_and_reversals(invoice_claims_df, pd_series)
    invoice_claims_df['basis_of_reimbursement_source'] = invoice_claims_df['basis_of_reimbursement_source'].astype(str)
    invoice_grouped_df = invoice_claims_df.groupby(['period_start', 'period_end', 'year', 'transaction_type', 'drug_type', 'basis_of_reimbursement_source'], as_index=False).aggregate({'ingredient_cost_paid_resp':'sum','total_paid_response':'sum','fill_reversal_indicator':'sum'}).rename(columns={'fill_reversal_indicator':'claim_count'})
    raw_invoices_df = invoices.set_net_invoice_number_and_date(invoice_grouped_df, period_start, period_end, chain.NET_NUMBER, chain.DUE_DATE, chain.INVOICE_STATIC_NUMBER)
    invoices_grouped_df = claims.calculate_sum_count(raw_invoices_df)
    for column in ['ingredient_cost_paid_resp','total_paid_response']:
        invoices_grouped_df[column] = utils.cast_cents_to_dollars(invoices_grouped_df, column, add_fill_reversal_sign=False)
    invoices_grouped_df = claims.calculate_claims_count(raw_invoices_df, invoices_grouped_df)
    invoices_grouped_df['grand total'] = claims.calculate_grand_total(invoices_grouped_df)  
    invoices_grouped_df = _select_columns_and_apply_formatting(invoices_grouped_df)
    return invoices_grouped_df

def _select_columns_and_apply_formatting(df):
    df = df[['period_start', 'period_end', 'invoice_date', 'due_date', 'net',
        'invoice_number', 'drug_type', 'basis_of_reimbursement_source', 'grand total', 'claims count', 'total paid claims',
        'total remunerative paid claims', 'ingredient_cost_paid_resp', 'total_paid_response']].rename(columns={'ingredient_cost_paid_resp':'total ingredient cost paid', 'total_paid_response':'total administration fee owed'})
    df[['total paid claims','total remunerative paid claims']] = df[['total paid claims','total remunerative paid claims']].astype('int')
    df = utils.apply_formatting(df, columns=['total administration fee owed', 'total ingredient cost paid', 'grand total'], formatting='{:,.2f}')
    return df
