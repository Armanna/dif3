from transform.walgreens import walgreens_claims
from transform import utils, claims, invoices
from sources import pharmacies
from sources import claims as sources_claims


def transform_walgreens_invoice_data(raw_claims_df, raw_ndchist_df, period_start, period_end):
    walgreens = pharmacies.Walgreens()
    raw_invoice_claims_df = sources_claims.join_ndcs_data(raw_claims_df, raw_ndchist_df)
    # filter dataframe by authorization_number
    invoice_claims_df = claims.filter_by_binnumber_authnumber(raw_invoice_claims_df, walgreens.BIN_NUMBERS)
    # modify dataframe to fit invoices file requirements 
    invoice_claims_df['period_start'] = period_start
    invoice_claims_df['period_end'] = period_end
    invoice_claims_df['year'] = period_start.year
    invoice_claims_df['basis_of_reimbursement_source'] = 'AWP'
    invoice_claims_df['transaction_type'] = claims.define_transaction_type(invoice_claims_df)
    invoice_claims_df['drug_type'] = walgreens.add_brand_generic_indicator(invoice_claims_df)
    invoice_claims_df['ingredient_cost_paid_resp'] = claims.calculate_ingredient_cost_paid(invoice_claims_df['transaction_type'], invoice_claims_df['ingredient_cost_paid_resp'])
    invoice_claims_df['total_paid_response'] = claims.calculate_administration_fee_owed(invoice_claims_df['transaction_type'], invoice_claims_df['total_paid_response'])
    invoice_claims_df['fill_reversal_indicator'] = claims.add_fills_reversals_indicator(invoice_claims_df, period_start, period_end)
    for col_name, pd_series in {'ingredient_cost_paid_resp':invoice_claims_df['ingredient_cost_paid_resp'], 'total_paid_response': -invoice_claims_df['total_paid_response']}.items():
        invoice_claims_df[col_name] = claims.apply_sign_for_fills_and_reversals(invoice_claims_df, pd_series)
    invoice_grouped_df = claims.group_and_calculate_metrics(invoice_claims_df)
    raw_walgreens_invoices_df = invoices.set_net_invoice_number_and_date(invoice_grouped_df, period_start, period_end, walgreens.NET_NUMBER, walgreens.DUE_DATE, walgreens.INVOICE_STATIC_NUMBER)
    walgreens_invoices_grouped_df = claims.calculate_sum_count(raw_walgreens_invoices_df)
    for column in ['ingredient_cost_paid_resp','total_paid_response']:
        walgreens_invoices_grouped_df[column] = utils.cast_cents_to_dollars(walgreens_invoices_grouped_df, column, add_fill_reversal_sign=False)
    walgreens_invoices_grouped_df = claims.calculate_claims_count(raw_walgreens_invoices_df, walgreens_invoices_grouped_df)
    walgreens_invoices_grouped_df['grand total'] = claims.calculate_grand_total(walgreens_invoices_grouped_df)  
    walgreens_invoices_grouped_df = _select_columns_andapply_formatting(walgreens_invoices_grouped_df)
    return walgreens_invoices_grouped_df

def _select_columns_andapply_formatting(df):
    df = df[['period_start', 'period_end', 'invoice_date', 'due_date', 'net',
        'invoice_number', 'drug_type', 'basis_of_reimbursement_source', 'grand total', 'claims count', 'total paid claims',
        'total remunerative paid claims', 'ingredient_cost_paid_resp', 'total_paid_response']].rename(columns={'ingredient_cost_paid_resp':'total ingredient cost paid', 'total_paid_response':'total administration fee owed'})
    df[['total paid claims','total remunerative paid claims']] = df[['total paid claims','total remunerative paid claims']].astype('int')
    utils.apply_formatting(df, columns=['total administration fee owed', 'total ingredient cost paid', 'grand total'], formatting='{:,.2f}')
    return df
