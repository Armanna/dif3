#!/usr/bin/env python3
from decimal import Decimal

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
from transform.publix import summary

from . import invoice_utils as iu

class PublixMarketplace(pharmacies.Publix):
    BIN_NUMBERS = ['026465']

chain_name = 'publix'
contract_name = 'markeplace'
publix_marketplace_class = PublixMarketplace

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, period, manual_date_string, **kwargs):
    log = logger.getLogger(f'Processing Publix Marketplace {period}ly')
    log.info(f"Running date is {manual_date_string}.")

    period_start, period_end = utils.process_invoice_dates(period_flag=period, manual_date_str=manual_date_string)
    today = dt.strptime(manual_date_string, "%Y-%m-%d") if manual_date_string not in [None, 'None'] else dt.now()

    if period == 'year':
        if today.year == 2026:
            # Marketplace live date
            period_start = publix_marketplace_class.MARKETPLACE_CONTRACT_START_DATE

    log.info(f"Period Start: {period_start}, Period End: {period_end}")

                                        ### DOWNLOAD AND PROCESS CLAIMS ###
    claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end,
                                                    fills_and_reversals = FillsAndReversals.FILLS, fill_status=FillStatus.FILLED, chains=Chains.PUBLIX, partners=Partners.ALL, print_sql_flag=True)
    raw_claims_df = claims_src.pull_data()

    claims_df = _preprocess_claims_df(raw_claims_df, period_start, period_end) # apply all necessary filters and conditions for specific chain

                                        ### DOWNLOAD AND PROCESS SOURCE FILES ###
    src = SourceDownloader(tables=[HistoricalData.PHARMACY_S3, HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3,
                                   MediSpan.NAME_S3, MediSpan.NDC_S3, Reporting.CHAIN_RATES_RS],
                                    get_modified_dfs=True, price_bin=publix_marketplace_class.BIN_NUMBERS[0])

    source_dict = src.generate_sources_dictionary()

    claims_processing_fees_src = claims_processing_fees.ClaimsProcFeesSource(period_start, period_end, chain_name)
    source_dict['claim_processing_fees'] = claims_processing_fees_src.downloader.pull_claim_processing_fees(sql_text=claims_processing_fees_src.invoice_sql)

                                        ### PROCESS TRANSACTIONS ###
    # build transactions dataframe
    transaction_raw_df = summary.build_default_transactions_dataset(claims_df, source_dict, chain_name, publix_marketplace_class, period_start, period_end)

                                        ### VALIDATE PRICE BASIS ###
    # need this validation in order to ensure that we don't blindly reconcile agains reconciliation_price_basis
    # if any descripancies found - we will receive notification in #temp_invoices slack channel and decide is it expected or not
    validation_dict = utils.validate_price_basis(transaction_raw_df, period_start, period_end, invoice_bucket, chain_name)

    transaction_results = transaction_raw_df.copy()
    transaction_final_df = _select_fields_for_exporting(transaction_results)
    transaction_file_name = utils.transactions_file_name(chain_name=chain_name.upper(), report_type=period)

                                        ### PROCESS SUMMARY ###
    log.info('processing: GER/BER info')
    awp_results, nadac_results = _calculate_ger_and_ber_metrics(transaction_raw_df)

    summary_results = summary.create_report_dict(awp_results, nadac_results)

    log.info(f'processing: {period}ly summary')
    date_period = f'Period: {period_start.strftime("%Y-%m-%d")} - {period_end.strftime("%Y-%m-%d")}'
    summary_bytes = summary.write_reconciliation_to_excel(summary_results, date_period)
    summary_results = pd.concat(summary_results.values(), axis=0, ignore_index=True)

                                        ### SEND FILES ###
    to_send_dict = {
        'summary_report': {
            'df': summary_results,
            'bytes': summary_bytes,
            'file_name': f"Hippo_{chain_name}_{contract_name}_{period.capitalize()}ly_Summary_" + dt.today().strftime("%m%d%Y") + ".xlsx"
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
    # FILTER BY ONLY MARKETPLACE CLAIMS
    # publix_marketplace_class
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, publix_marketplace_class.BIN_NUMBERS)                           # filter dataframe by bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= publix_marketplace_class.MARKETPLACE_CONTRACT_START_DATE]                # go live for Publix contract 2024-08-01
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)
    return claims_df

def _select_fields_for_exporting(transactions_df):
    transactions_df['Claim authorization number'] = '="' + transactions_df['authorization_number'].fillna('') + '"' # needed for excel to treat data as text not number
    transactions_df['product_id'] = transactions_df['product_id'].astype('string').str.zfill(11)
    transactions_df['NDC'] = '="' + transactions_df['product_id'].fillna('') + '"' # needed for excel to treat data as text not number
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['claim_date_of_service'] = transactions_df['claim_date_of_service'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df = transactions_df.rename(columns=publix_marketplace_class.COLUMN_RENAME_DICT) # rename columns
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
            "Control Schedule",
            "Total AWP",
            "Total NADAC",
            "Total WAC",
            "Excluded Reason",
            "Brand/Generic Medi-Span indicator field",
            "Transmission porcessing fee"
        ]
    ]
    return transactions_df

def _calculate_ger_and_ber_metrics(df, process_reversals_flag=True):
    if process_reversals_flag:
        mask = df['Excluded Claim Indicator'] == 'N'
    else:
        mask = (df['Excluded Claim Indicator'] == 'N') & (df['reversal indicator'] != 'B2')

    df = df[mask]
    if df.empty:
        return pd.DataFrame()

    awp_transactions = df[df['reconciliation_price_basis'] == 'AWP']
    nadac_transactions = df[df['reconciliation_price_basis'] == 'NADAC']

    awp_grouped = awp_transactions.groupby(['drug_type', 'reconciliation_price_basis', 'target_rate', 'dfer_target_rate'], as_index=False).agg({
        'authorization_number': 'count',
        'contracted_cost': 'sum',
        'full_cost': 'sum',
        'ingredient_cost_paid_resp': 'sum',
        'dispensing_fee_paid_resp': 'sum',
        'total_paid_response': 'sum'
    }).rename(columns={'authorization_number': 'Total claims'})

    awp_grouped = awp_grouped[awp_grouped['Total claims'] > 0]
    awp_grouped['Actual rate'] = ((1 - awp_grouped['ingredient_cost_paid_resp'] / awp_grouped['full_cost']) * Decimal('100')).apply(lambda x: x.quantize(Decimal('.01')))
    awp_grouped['Ingr Cost Dollar variance'] = (awp_grouped['ingredient_cost_paid_resp'] - awp_grouped['contracted_cost']).apply(lambda x: x.quantize(Decimal('.01')))
    awp_grouped['Contracted Dispensing Fee'] = awp_grouped['Total claims'] * awp_grouped['dfer_target_rate']
    awp_grouped['Dispensing fee variance'] = (awp_grouped['dispensing_fee_paid_resp'] - awp_grouped['Contracted Dispensing Fee']).apply(lambda x: x.quantize(Decimal('.01')))
    awp_grouped = awp_grouped[['drug_type', 'reconciliation_price_basis', 'Total claims', 'contracted_cost', 'full_cost',
                    'ingredient_cost_paid_resp', 'Actual rate', 'target_rate', 'dfer_target_rate',
                    'dispensing_fee_paid_resp', 'Contracted Dispensing Fee', 'Dispensing fee variance',
                    'Ingr Cost Dollar variance', 'total_paid_response']]

    nadac_grouped = nadac_transactions.groupby(
        ['drug_type', 'reconciliation_price_basis', 'claim_program_name', 'target_rate', 'dfer_target_rate'], as_index=False).agg({
        'authorization_number': 'count',
        'contracted_cost': 'sum',
        'full_cost': 'sum',
        'ingredient_cost_paid_resp': 'sum',
        'dispensing_fee_paid_resp': 'sum',
        'total_paid_response': 'sum'
    }).rename(columns={'authorization_number': 'Total claims'})
    nadac_grouped = nadac_grouped[nadac_grouped['Total claims'] > 0]
    nadac_grouped['Actual rate'] = (
                (1 - nadac_grouped['ingredient_cost_paid_resp'] / nadac_grouped['full_cost']) * Decimal('100')).apply(
        lambda x: x.quantize(Decimal('.01')))
    nadac_grouped['Ingr Cost Dollar variance'] = (
                nadac_grouped['ingredient_cost_paid_resp'] - nadac_grouped['contracted_cost']).apply(
        lambda x: x.quantize(Decimal('.01')))
    nadac_grouped['Contracted Dispensing Fee'] = nadac_grouped['Total claims'] * nadac_grouped['dfer_target_rate']
    nadac_grouped['Dispensing fee variance'] = (
                nadac_grouped['dispensing_fee_paid_resp'] - nadac_grouped['Contracted Dispensing Fee']).apply(
        lambda x: x.quantize(Decimal('.01')))
    nadac_grouped = nadac_grouped[
        ['drug_type', 'reconciliation_price_basis', 'claim_program_name', 'Total claims', 'contracted_cost', 'full_cost',
         'ingredient_cost_paid_resp', 'Actual rate', 'target_rate', 'dfer_target_rate',
         'dispensing_fee_paid_resp', 'Contracted Dispensing Fee', 'Dispensing fee variance',
         'Ingr Cost Dollar variance', 'total_paid_response']]

    return awp_grouped, nadac_grouped
