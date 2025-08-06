#!/usr/bin/env python3

import numpy as np
import pandas as pd
from datetime import datetime as dt
import copy

from sources import claims as sources_claims
from transform import claims as transform_claims
from transform import utils
from hippo.sources.download_sources import Reporting, SourceDownloader, HistoricalData, MediSpan
import sources.r_claim_proc_fees as claims_processing_fees
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, Partners, FillStatus
from sources import pharmacies
from hippo import logger
from . import invoice_utils as iu


log = logger.getLogger('Processing Publix DTC quarterly.') 


class PublixDTC(pharmacies.Publix):
    BIN_NUMBERS = ['019876']

chain_name = 'publix'
publix_dtc_class = PublixDTC

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    period_start, period_end = utils.process_invoice_dates(period_flag='quarter')

                                        ### DOWNLOAD AND PROCESS CLAIMS ###
    claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS_AND_OUT_OF_MONTH_REVERSALS, fill_status=FillStatus.FILLED, chains=Chains.PUBLIX, partners=Partners.ALL, print_sql_flag=True)
    raw_claims_df = claims_src.pull_data()
    claims_df = _preprocess_claims_df(raw_claims_df, period_start, period_end) # apply all necessary filters and conditions for specific chain

                                        ### DOWNLOAD AND PROCESS SOURCE FILES ###
    src = SourceDownloader(tables=[HistoricalData.PHARMACY_S3, HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3, MediSpan.NAME_S3, MediSpan.NDC_S3, Reporting.CHAIN_RATES_RS], get_modified_dfs=True) 
    source_dict = src.generate_sources_dictionary()
    claims_processing_fees_src = claims_processing_fees.ClaimsProcFeesSource(period_start, period_end, chain_name)
    source_dict['claim_processing_fees'] = claims_processing_fees_src.downloader.pull_claim_processing_fees(sql_text=claims_processing_fees_src.invoice_sql)

                                        ### PROCESS TRANSACTIONS ###
    # build transactions dataframe
    transaction_raw_df = transform_claims.build_default_transactions_dataset(claims_df, source_dict, chain_name, publix_dtc_class, period_start, period_end)

                                        ### VALIDATE PRICE BASIS ###
    # need this validation in order to ensure that we don't blindly reconcile agains reconciliation_price_basis
    # if any descripancies found - we will receive notification in #temp_invoices slack channel and decide is it expected or not
    validation_dict = utils.validate_price_basis(transaction_raw_df, period_start, period_end, invoice_bucket, chain_name)

    transaction_results = transaction_raw_df.copy()
    transaction_final_df = _select_fields_for_exporting(transaction_results)
    transaction_file_name = utils.transactions_file_name(chain_name=chain_name.upper(), report_type='quarterly')

                                        ### PROCESS QUARTERLY SUMMARY ###
    log.info('processing: GER/BER info')
    ger_results = utils.calculate_ger_and_ber_metrics(transaction_raw_df)
    summary_results = utils.create_report_dict(ger_results)

    log.info('processing: quarterly summary')
    date_period = f'Period: {period_start.strftime("%Y-%m-%d")} - {period_end.strftime("%Y-%m-%d")}'
    summary_bytes = utils.write_quarterly_reconciliation_to_excel(summary_results, date_period)
    summary_results = pd.concat(summary_results.values(), axis=0, ignore_index=True)

                                        ### SEND FILES ###
    to_send_dict = {
        'summary_report': {
            'df': summary_results,
            'bytes': summary_bytes,
            'file_name': f"Hippo_{chain_name}_Quarterly_Summary_" + dt.today().strftime("%m%d%Y") + ".xlsx"
        },
    }
    for name, bytes in to_send_dict.items():
        if bytes != None:
            iu.send_file(bytes['df'], chain_name, bytes['file_name'], bytes['bytes'], slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)
        else:
            log.info("no data for file" + name)

    if validation_dict['dataframe'].empty == False:
        utils.send_text_message_to_slack(kwargs.get('temp_slack_channel'), slack_bot_token, validation_dict['message'])

    iu.create_and_send_file("transactions", transaction_final_df, transaction_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain=chain_name.upper(), hsdk_env=hsdk_env, **kwargs)

def _preprocess_claims_df(raw_claims_df, period_start, period_end):
    claims_df = sources_claims.preprocess_claims(raw_claims_df, partners=True)
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, publix_dtc_class.BIN_NUMBERS)                           # filter dataframe by bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= publix_dtc_class.CONTRACT_START_DATE]                # go live for Publix contract 2024-08-01
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)
    return claims_df

def _select_fields_for_exporting(transactions_df):
    transactions_df['Claim authorization number'] = '="' + transactions_df['authorization_number'].fillna('') + '"' # needed for excel to treat data as text not number
    transactions_df['product_id'] = transactions_df['product_id'].astype('string').str.zfill(11)
    transactions_df['NDC'] = '="' + transactions_df['product_id'].fillna('') + '"' # needed for excel to treat data as text not number
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['claim_date_of_service'] = transactions_df['claim_date_of_service'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df = transactions_df.rename(columns=publix_dtc_class.COLUMN_RENAME_DICT) # rename columns
    transactions_df['Reversal date'] = np.where(transactions_df['reversal indicator'] != 'B2', '', transactions_df['Reversal date']) # set empty string instead of 2050-01-01 for fills
    transactions_df[["Vaccine/Immunization Administration fee paid"]] = ''
    transactions_df = transactions_df[
        [
            "BIN",
            "Plan carrier code",
            "Claim authorization number",
            "NCPDP",
            "Prescription number",
            "Claim reference number",
            "Date of Service",
            "Paid claim response date",
            "Reversal date",
            "NPI",
            "NDC",
            "Paid Ingredient Cost",
            "Paid Dispense Fee",
            "Vaccine/Immunization Administration fee paid",
            "Tax paid",
            "Total quantity dispensed",
            "Usual and Customary Charge (U&C)",
            "Patient Paid Amount",
            "Administration Fee",
            "Basis of reimbursement source",            
            "Total AWP",
            "Total NADAC",
            "Total WAC",
            "Brand/Generic Medi-Span indicator field",
            "Transmission porcessing fee"
        ]
    ]
    return transactions_df
