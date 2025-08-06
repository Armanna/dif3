#!/usr/bin/env python3

import numpy as np

from sources import claims as sources_claims
from transform import claims as transform_claims, draw_invoice
from transform import utils
from hippo.sources.download_sources import Reporting, SourceDownloader, HistoricalData, MediSpan
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, Partners, FillStatus
from sources import pharmacies

from . import invoice_utils as iu

chain_name = 'meijer'
chain_class = pharmacies.Meijer

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    period_start, period_end = utils.process_invoice_dates(period_flag='month')

    # downloads and process claims
    claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS_AND_OUT_OF_MONTH_REVERSALS, fill_status=FillStatus.FILLED, chains=Chains.MEIJER, partners=Partners.ALL, print_sql_flag=True)
    raw_claims_df = claims_src.pull_data()
    claims_df = _preprocess_claims_df(raw_claims_df, period_start, period_end) # apply all necessary filters and conditions for specific chain

    # downloads source files
    src = SourceDownloader(tables=[HistoricalData.PHARMACY_S3, HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3, MediSpan.NAME_S3, MediSpan.NDC_S3, Reporting.CHAIN_RATES_RS], get_modified_dfs=True) 
    source_dict = src.generate_sources_dictionary()

                                        ### PROCESS TRANSACTIONS ###
    transaction_results = transform_claims.build_default_transactions_dataset(claims_df, source_dict, chain_name, chain_class, period_start, period_end)


    transaction_final_df = _select_fields_for_exporting(transaction_results)
    transaction_file_name = utils.transactions_file_name(chain_name=chain_name.upper())

                                        ### VALIDATE PRICE BASIS ###
    # need this validation in order to ensure that we don't blindly reconcile agains reconciliation_price_basis
    # if any descripancies found - we will receive notification in #temp_invoices slack channel and decide is it expected or not
    validation_dict = utils.validate_price_basis(transaction_results, period_start, period_end, invoice_bucket, chain_name)

                                        ### PROCESS INVOICES ###
    invoice_results = transform_claims.build_default_invoice_data(transaction_results, chain_class, period_start, period_end)
    invoices_grouped_df = _select_invoice_columns_and_apply_formatting(invoice_results)
    
                                        ### DRAW INVOICE ###
    file_bytes, send_file_name = draw_invoice.create_invoice(invoices_grouped_df, chain_name.capitalize(), chain_class)

                                        ### VALIDATE RESULTS ###
    sources_claims.test_invoice_results_vs_transaction_final(invoice_results, transaction_results)

                                        ### SEND FILES ###
    assert file_bytes != None, "No data for invoice file creation"

    if validation_dict['dataframe'].empty == False:
        utils.send_text_message_to_slack(kwargs.get('temp_slack_channel'), slack_bot_token, validation_dict['message'])

    iu.send_file(invoice_results, chain_name.upper(), send_file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)    

    iu.create_and_send_file("transactions", transaction_final_df, transaction_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain=chain_name.upper(), hsdk_env=hsdk_env, **kwargs)

def _preprocess_claims_df(raw_claims_df, period_start, period_end):
    claims_df = sources_claims.preprocess_claims(raw_claims_df, partners=True)
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, chain_class.BIN_NUMBERS)                           # filter dataframe by bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= chain_class.CONTRACT_START_DATE]                # go live for Mejer contract 2025-01-01
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)

    return claims_df

def _select_fields_for_exporting(transactions_df):
    transactions_df['BIN+PCN+GROUP'] = transactions_df['bin_number'].astype('object').fillna('') + '+' + transactions_df['process_control_number'].fillna('') + '+' + transactions_df['group_id'].fillna('')
    transactions_df['Claim Authorization Number'] = '="' + transactions_df['authorization_number'].fillna('') + '"' # needed for excel to treat data as text not number
    transactions_df['product_id'] = transactions_df['product_id'].astype('string').str.zfill(11)
    transactions_df['NDC'] = '="' + transactions_df['product_id'].fillna('') + '"' # needed for excel to treat data as text not number
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Fill date'] = transactions_df['valid_from'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['claim_date_of_service'] = transactions_df['claim_date_of_service'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Reversal date'] = np.where(transactions_df['reversal indicator'] != 'B2', '', transactions_df['Reversal date']) # set empty string instead of 2050-01-01 for fills
    transactions_df = transactions_df.rename(columns=chain_class.COLUMN_RENAME_DICT) # rename columns 
    transactions_df = transactions_df[chain_class.REQUESTED_COLUMNS]

    return transactions_df

def _select_invoice_columns_and_apply_formatting(df):
    df = df[['period_start', 'period_end', 'invoice_date', 'due_date', 'net', 'invoice_number', 
                'drug_type', 'basis_of_reimbursement_source', 'grand total', 'claims count', 'total paid claims',
                'total remunerative paid claims', 'ingredient_cost_paid_resp', 'total_paid_response']].rename(columns={'ingredient_cost_paid_resp':'total ingredient cost paid', 'total_paid_response':'total administration fee owed'})
    df[['total paid claims','total remunerative paid claims']] = df[['total paid claims','total remunerative paid claims']].astype('int')
    df = utils.apply_formatting(df, columns=['total administration fee owed', 'total ingredient cost paid', 'grand total'], formatting='{:,.2f}')
    return df
