#!/usr/bin/env python3

import pandas as pd
from sources import claims as sources_claims
from transform import claims as transform_claims, draw_invoice
from transform import utils
from transform.kroger_and_harris_teeter import bi_weekly_invoice, transactions
from hippo.sources.download_sources import SourceDownloader, HistoricalData, MediSpan, HistoricalDataPbmHippo
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, FillStatus
from sources import pharmacies

from . import invoice_utils as iu

chain_class = pharmacies.KrogerAndHarrisTeeter

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, chain_name, **kwargs):

    period_start, period_end = utils.process_invoice_dates(period_flag='bi-weekly')
    # manually handle exclusion for May 2024 because we started processing Kroger claims within direct deal at 15th of May while standard invoicing periods are 1st-15th, 16th-the_end_of_the_month_date; so instead of 2024-05-16 - 2024-05-31 we need to process 2024-05-15 - 2024-05-31
    period_start = pd.to_datetime('2024-05-15') if period_end == pd.to_datetime('2024-05-31') else period_start

    if chain_name == 'kroger':
        chain_enum = Chains.KROGER
    elif chain_name == 'harris_teeter':
        chain_enum = Chains.HARRIS_TEETER
    
    # downloads and process claims
    claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS_AND_REVESALS, fill_status=FillStatus.FILLED, chains=chain_enum, print_sql_flag=True)
    raw_claims_df = claims_src.pull_data()
    claims_df = preprocess_claims_df(raw_claims_df, period_start, period_end) # apply all necessary filters and conditions for specific chain
    # downloads source files
    src = SourceDownloader(tables=[HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3,
                                   HistoricalData.DRUG_NDCS_WITH_RXNORM, MediSpan.NAME_S3, MediSpan.NDC_S3,
                                   HistoricalDataPbmHippo.KROGER_MAC_CS_2_HISTORY, HistoricalDataPbmHippo.KROGER_MAC_CS_3_4_5_HISTORY], get_modified_dfs=True)
    source_dict = src.generate_sources_dictionary()
                                        
                                        ### PROCESS INVOICES ###
    invoice_results = bi_weekly_invoice.transform_invoice_data(claims_df, source_dict['ndcs_v2'], source_dict['mf2ndc'], period_start, period_end)
    # draw invoice PDF
    file_bytes, send_file_name = draw_invoice.create_invoice(invoice_results, chain_name.capitalize(), chain_class)
    
                                        ### PROCESS TRANSACTIONS ###
    transaction_results = transactions.transform_transactions_data(claims_df, source_dict, period_start, period_end)
    transaction_results = _select_fields_for_exporting(transaction_results)
    transaction_file_name = utils.transactions_file_name(chain_name=chain_name.upper())

                                        ### VALIDATE RESULTS ###
    iu.validate_results(invoice_results, transaction_results.rename(columns={'Paid Ingredient Cost':'Ingredient Cost Paid'}))

                                        ### SEND FILES ###
    assert file_bytes != None, "No data for invoice file creation"
        
    iu.send_file(invoice_results, chain_name.upper(), send_file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)    

    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain=chain_name.upper(), hsdk_env=hsdk_env, **kwargs)

def preprocess_claims_df(raw_claims_df, period_start, period_end):
    claims_df = sources_claims.preprocess_claims(raw_claims_df)
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, chain_class.BIN_NUMBERS, chain_class.CHAIN_CODES)        # filter dataframe by chan_code, bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= chain_class.CONTRACT_START_DATE]                # go live for Kroger contract 2024-05-15
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)

    return claims_df

def _select_fields_for_exporting(transactions_df):
    transactions_df = transactions_df[['BIN', 'PCN', 'Network Reimbursement ID', 'Claim number', 'Date of Service', 'Fill date', 'Refill number', 'Drug name', 'Total quantity dispensed', 'NDC', 
    'Basis of reimbursement determination', 'NPI', "Days' supply", 'Prescription number', 'Administration Fee', 'Paid Ingredient Cost', 'Paid Dispense Fee', 'Patient Paid Amount','claim_type','generic indicator (multi-source indicator)','Reversal date', 'reversal indicator', 'Amount due to Vendor', 'Amount due to Pharmacy', 'MONY code']]
    return transactions_df
