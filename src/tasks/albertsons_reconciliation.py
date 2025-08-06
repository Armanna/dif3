#!/usr/bin/env python3

import numpy as np
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from sources import claims as sources_claims, pharmacies
from transform import claims as transform_claims, utils
from tasks import download_other_sources
from hippo import logger
from hippo.sources.s3 import S3Downloader
from . import invoice_utils as iu

log = logger.getLogger('Processing Albertsons reconciliation') 

chain_name = 'albertsons'
chain_class = pharmacies.Albertsons

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, period, manual_date_string, **kwargs):
    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")
    log.info(f"Running {period}ly report.")
    log.info(f"Running date is {manual_date_string}.")
    temp_main_prefix = f"temp/sources/albertsons/{period}/"
    temp_claims_prefix = f"temp/claims/albertsons/{period}"
    
    period_start, period_end = utils.process_invoice_dates(period_flag=period, manual_date_str=manual_date_string)
    # manually handle exclusion for the year 2025 because we oficially started processing Albertsons claims within direct deal at 1st of April
    # contract says that we must prvide quarterly data srating 1st day of the year or contract Effective Date (whatever is later) and untill the last day of the quarter
    period_start = pd.to_datetime('2025-04-01') if period_end.year == 2025 else period_start
    
    log.info(f"Downloading the data for the period: {period_start} - {period_end}.")
    raw_claims_df = S3Downloader(invoice_bucket, temp_claims_prefix, 'claims.parquet').pull_parquet()[0]
    claims_df = _preprocess_claims_df(raw_claims_df, period_start, period_end)
    log.info(f"Processing default source dictionary.")
    source_dict = download_other_sources.load_parquet_files_to_dict(invoice_bucket, temp_main_prefix)

                                        ### PROCESS TRANSACTIONS ###
    transaction_raw_df = transform_claims.build_default_transactions_dataset(claims_df, source_dict, chain_name, chain_class, period_start, period_end)
    transaction_final_df = _select_fields_for_exporting(transaction_raw_df)
    transaction_file_name = transaction_file_name = utils.transactions_file_name(chain_name=chain_name.upper(), report_type='quarterly', period_start=period_start.strftime('%Y-%m-%d')+'_')

                                        ### VALIDATE PRICE BASIS ###
    # need this validation in order to ensure that we don't blindly reconcile agains reconciliation_price_basis
    # if any descripancies found - we will receive notification in #temp_invoices slack channel and decide is it expected or not
    validation_dict = utils.validate_price_basis(transaction_raw_df, period_start, period_end, invoice_bucket, chain_name)

                                        ### PROCESS QUARTERLY SUMMARY ###
    log.info('processing: GER/BER info')
    ger_results = utils.calculate_ger_and_ber_metrics(transaction_raw_df)
    summary_results = utils.create_report_dict(ger_results, dispensing_fee_flag=True)

    log.info('processing: quarterly summary')
    date_period = f'Period: {period_start.strftime("%Y-%m-%d")} - {period_end.strftime("%Y-%m-%d")}'
    summary_bytes = utils.write_quarterly_reconciliation_to_excel(summary_results, date_period, dispensing_fee_report_flag=True)
    summary_results = pd.concat(summary_results.values(), axis=0, ignore_index=True)
    summary_file_name = utils.transactions_file_name(chain_name=chain_name.upper(), report_type='summary', period_start=period_start.strftime('%Y-%m-%d')+'_')
                                        ### SEND FILES ###
    to_send_dict = {
        'summary_report': {
            'df': summary_results,
            'bytes': summary_bytes,
            'file_name': summary_file_name
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
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, chain_class.BIN_NUMBERS)                           # filter dataframe by bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= chain_class.CONTRACT_START_DATE]                # go live for Albertsons contract 2025-04-01
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)

    return claims_df

def _select_fields_for_exporting(transactions_df):
    transactions_df['BIN+PCN+GROUP'] = transactions_df['bin_number'].astype('object').fillna('') + '+' + transactions_df['process_control_number'].fillna('') + '+' + transactions_df['group_id'].fillna('')
    transactions_df['Claim Authorization Number'] = transactions_df['authorization_number'].fillna('')
    transactions_df['product_id'] = transactions_df['product_id'].astype('string').str.zfill(11)
    transactions_df['NDC'] = transactions_df['product_id'].fillna('')
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Fill date'] = transactions_df['valid_from'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['claim_date_of_service'] = transactions_df['claim_date_of_service'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Reversal date'] = np.where(transactions_df['reversal indicator'] != 'B2', '', transactions_df['Reversal date']) # set empty string instead of 2050-01-01 for fills
    transactions_df['NADAC Indicator'] = np.where(transactions_df['price_basis'] == 'NADAC', 'Y', 'N')
    transactions_df['Revenue Share'] = transactions_df.apply(_calculate_revenue_share, axis=1)
    transactions_df['Revenue Share'] = transactions_df['Revenue Share'] * transactions_df['fill_reversal_indicator']
    transactions_df = transactions_df.rename(columns=chain_class.COLUMN_RENAME_DICT) # rename columns 
    transactions_df = transactions_df[chain_class.REQUESTED_COLUMNS]

    return transactions_df

def _calculate_revenue_share(row):
    admin_fee_floor = Decimal(chain_class.ADMIN_FEE_SHARE_FLOR)
    revenue_share_percent = Decimal(chain_class.REVENUE_SHARE_OVER_MARGIN_PERCENT)
    paid = abs(Decimal(row['total_paid_response']))
    if paid > admin_fee_floor: # if Admin Fee collected is more than $5
        share = revenue_share_percent * (paid - admin_fee_floor) # share 50% of the profit above $5 with Albertsons
        return share.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) # round 0.115 to 0.12
    else:
        return Decimal('0.00') # if Admin Fee is $5 or lower - all margin goes to the Hippo = nothig to share with Albertsons
