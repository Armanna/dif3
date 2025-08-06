#!/usr/bin/env python3

import numpy as np
import pandas as pd
from datetime import datetime as dt

from sources import claims as sources_claims
from transform import claims as transform_claims
from transform.publix import draw_invoice_publix
from transform import utils
from hippo.sources.download_sources import Reporting, SourceDownloader, HistoricalData, MediSpan
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, Partners, FillStatus
from sources.pharmacies import Pharmacies, Publix

from . import invoice_utils as iu

chain_name = 'publix'
chain_class = Publix

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, manual_date_string, **kwargs):

    period_start, period_end = utils.process_invoice_dates(period_flag='month', manual_date_str=manual_date_string)
    # downloads and process claims
    claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS_AND_REVESALS, fill_status=FillStatus.FILLED, chains=Chains.PUBLIX, partners=Partners.ALL, print_sql_flag=True)
    raw_claims_df = claims_src.pull_data()
    claims_df = _preprocess_claims_df(raw_claims_df, period_start, period_end) # apply all necessary filters and conditions for specific chain

    # downloads source files
    src = SourceDownloader(tables=[HistoricalData.PHARMACY_S3, HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3, Reporting.CHAIN_RATES_RS], get_modified_dfs=True) 
    source_dict = src.generate_sources_dictionary()
    
    chain_specific_src = SourceDownloader(tables=[MediSpan.NDC_S3, MediSpan.NAME_S3], get_modified_dfs=True) 
    chain_specific_dict = chain_specific_src.generate_sources_dictionary()
    
    claims_df = sources_claims.join_default_source_dataframes(claims_df, chain_specific_dict)

                                        ### PROCESS TRANSACTIONS ###
    mask = (                                                                                # the way to process 14 MAC transactions occured on August 1st by mistake
        (claims_df.basis_of_reimbursement_determination_resp == '07') &                     # since there is no MAC rates in reporting chain_rate - the idea is to temporary change basis_of_reimbursement to UNC
        (claims_df.claim_date_of_service == pd.to_datetime('2024-08-01'))                   # and then restore it back to MAC after transaction dataset will be created
    )                                                                                       # in that case no rates will be attached to this transactions but they will exist in transactions file as MAC transaction
    affected_authorization_numbers = claims_df.loc[mask, 'authorization_number'].copy()
    claims_df.loc[mask, 'basis_of_reimbursement_determination_resp'] = '04'
    # build transactions dataframe
    transaction_results = transform_claims.build_default_transactions_dataset(claims_df, source_dict, chain_name, chain_class, period_start, period_end)

    # restore MAC basis_of_reimbursement for 14 MAC transactions
    transaction_results.loc[transaction_results.authorization_number.isin(affected_authorization_numbers), 'price_basis'] = 'MAC'
    
    # RiteAid set specific rates for Schedule II drugs BUT ONLY for Marketplace
    # Before processing transactions we need to split BIN 026465 claims to Schedule II/ non-Schedule II
    claims_df['program_name'] = _define_category(claims_df[['dea_class_code', 'bin_number']])

    # build transactions dataframe
    transaction_results = transform_claims.build_default_transactions_dataset(claims_df, source_dict, chain_name, chain_class, period_start, period_end)

    transaction_final_df = _select_fields_for_exporting(transaction_results)

                                        ### VALIDATE PRICE BASIS ###
    # need this validation in order to ensure that we don't blindly reconcile agains reconciliation_price_basis
    # if any descripancies found - we will receive notification in #temp_invoices slack channel and decide is it expected or not
    validation_dict = utils.validate_price_basis(transaction_results, period_start, period_end, invoice_bucket, chain_name)

    invoices_dict = {}
                                        ### PROCESS INVOICES ###
            ### INCLUDE TWO BINS: Regular Publix: '019876' and Publix Marketplace'026465'] ###
    for bin_number in chain_class.BIN_NUMBERS:
        invoice_per_bin_df = transaction_results[(~transaction_results.authorization_number.isin(affected_authorization_numbers)) & (transaction_results.bin_number == bin_number)]
        invoice_results = transform_claims.build_default_invoice_data(invoice_per_bin_df, chain_class, period_start, period_end)
        _validate_results(transaction_results, invoice_results, period_start, bin_number) # validate invoice results against transactions
        invoices_grouped_df = _select_invoice_columns_and_apply_formatting(invoice_results)
        invoices_dict[bin_number] = invoices_grouped_df

    combined_df = _prepare_combined_invoice_df(invoices_dict['019876'], invoices_dict['026465'])

    file_bytes = draw_invoice_publix.create_invoice(
        combined_df[combined_df['basis_of_reimbursement_source'] != 'UNC'], chain_name.capitalize(), chain_class,
        business_labels=["Publix DTC", "Publix Marketplace"]
    )

    transaction_file_name = utils.transactions_file_name(chain_name=chain_name.upper(), report_type='invoice_utilization', period_start=period_start.strftime('%Y-%m-%d')+'_')
    invoice_file_name = period_start.strftime('%Y-%m-%d')+"_Hippo_" + chain_name + f"_invoice_" + dt.today().strftime("%m%d%Y") + ".pdf"
    
                                            ### SEND FILES ###
    assert file_bytes != None, "No data for invoice file creation"

    if validation_dict['dataframe'].empty == False:
        utils.send_text_message_to_slack(kwargs.get('temp_slack_channel'), slack_bot_token, validation_dict['message'])

    iu.send_file(invoice_results, chain_name.upper(), invoice_file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)    

    iu.create_and_send_file("transactions", transaction_final_df, transaction_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain=chain_name.upper(), hsdk_env=hsdk_env, **kwargs)

def _preprocess_claims_df(raw_claims_df, period_start, period_end):
    claims_df = sources_claims.preprocess_claims(raw_claims_df, partners=True)
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, chain_class.BIN_NUMBERS)                                 # filter dataframe by bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= chain_class.CONTRACT_START_DATE]                      # go live for Publix contract 2024-08-01
    claims_df = claims_df.loc[~((claims_df['bin_number'] == '026465') & 
                            (claims_df['claim_date_of_service'].dt.floor('D') < chain_class.MARKETPLACE_CONTRACT_START_DATE))]      # go live for Publix Marketplace contract 2025-03-31
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)

    return claims_df

def _select_fields_for_exporting(transactions_df):
    transactions_df['Claim authorization number'] = '="' + transactions_df['authorization_number'].fillna('') + '"' # needed for excel to treat data as text not number
    transactions_df['product_id'] = transactions_df['product_id'].astype('string').str.zfill(11)
    transactions_df['NDC'] = '="' + transactions_df['product_id'].fillna('') + '"' # needed for excel to treat data as text not number
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Fill date'] = transactions_df['valid_from'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['claim_date_of_service'] = transactions_df['claim_date_of_service'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df = transactions_df.rename(columns=chain_class.COLUMN_RENAME_DICT) # rename columns 
    transactions_df['Reversal date'] = np.where(transactions_df['reversal indicator'] != 'B2', '', transactions_df['Reversal date']) # set empty string instead of 2050-01-01 for fills
    transactions_df = transactions_df[
        [
            "BIN",
            "PCN",
            "Claim reference number",
            "Prescription number",
            "Refill number",
            "Fill date",
            "Reversal date",
            "Claim authorization number",
            "Date of Service",
            "NDC",
            "NPI",
            "Patient Paid Amount",
            "Administration Fee",
            "Total quantity dispensed",
            "Days' supply",
            "Paid Ingredient Cost",
            "Paid Dispense Fee",
            "Usual and Customary Charge (U&C)",
            "Tax paid",
            "Basis of reimbursement source"
        ]
    ]
    return transactions_df

def _select_invoice_columns_and_apply_formatting(df):
    df = df[['period_start', 'period_end', 'invoice_date', 'due_date', 'net', 'invoice_number', 
                'drug_type', 'basis_of_reimbursement_source', 'grand total', 'claims count', 'total paid claims',
                'total remunerative paid claims', 'ingredient_cost_paid_resp', 'total_paid_response']].rename(columns={'ingredient_cost_paid_resp':'total ingredient cost paid', 'total_paid_response':'total administration fee owed'})
    df[['total paid claims','total remunerative paid claims']] = df[['total paid claims','total remunerative paid claims']].astype('int')
    df = utils.apply_formatting(df, columns=['total administration fee owed', 'total ingredient cost paid', 'grand total'], formatting='{:,.2f}')
    return df

def _define_category(df: pd.DataFrame) -> pd.Series:
    mapping = {
        '2': 'Controlled Drug C-II'
    }
    result_series = pd.Series('', index=df.index)
    mask = df['bin_number'] == '026465'
    result_series.loc[mask] = df.loc[mask, 'dea_class_code'].map(mapping).fillna('')

    return result_series

def _validate_results(transaction_results, final_df, period_start, bin_number):
    transaction_validation_df = transaction_results[transaction_results['price_basis'] != 'MAC'] if period_start == pd.to_datetime('2024-08-01') else transaction_results # exclude MAC transactions for validation reasons
    transaction_validation_df = transaction_results[transaction_results['bin_number'] == bin_number]

    sources_claims.test_invoice_results_vs_transaction_final(final_df, transaction_validation_df)

def _prepare_combined_invoice_df(df1, df2):
    merge_keys = ['drug_type', 'basis_of_reimbursement_source']
    combined = df1.merge(df2, on=merge_keys, how='outer', suffixes=('_regular', '_marketplace'))

    numeric_cols = [
        'grand total', 'claims count',
        'total paid claims', 'total remunerative paid claims',
        'total ingredient cost paid', 'total administration fee owed'
    ]

    for col in numeric_cols:
        for suffix in ['_regular', '_marketplace']:
            full_col = col + suffix
            if full_col not in combined.columns:
                combined[full_col] = 0
            combined[full_col] = combined[full_col].fillna(0)

    meta_fields = ['period_start', 'period_end', 'invoice_date', 'due_date', 'net', 'invoice_number']
    for field in meta_fields:
        combined[field] = df1[field].combine_first(df2[field])

    return combined
