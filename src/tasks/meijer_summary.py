#!/usr/bin/env python3

import pandas as pd
import io
from datetime import datetime as dt
from decimal import Decimal

from sources import claims as sources_claims
from transform import claims as transform_claims
from transform import utils
from hippo.sources.download_sources import SourceDownloader, HistoricalData, MediSpan, Reporting
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, FillStatus, Partners
from sources import pharmacies
import sources.r_claim_proc_fees as claims_processing_fees


from hippo import logger

from . import invoice_utils as iu

chain_class = pharmacies.Meijer
chain_name = 'meijer'


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, period, manual_date_string, **kwargs):
    log = logger.getLogger(f'Processing Meijer {period}ly')

    period_start, period_end = utils.process_invoice_dates(period_flag=period, manual_date_str=manual_date_string)

    # Don't process the Q4 quarterly report
    if period=='quarter' and period_start.month >= 10:
        log.info('Skipping Q4 quarterly report')
        return

    # Quarterly reports always process previous quarters / also works for the year
    period_start = pd.to_datetime(f'{period_start.year}-01-01')

    log.info(f'Processing Meijer {period}ly report for {period_start} to {period_end}')


    chain_enum = Chains.MEIJER

    # downloads and process claims
    # Download only fills as the contract asks to exclude reversals
    claims_src = claims_downloader.ClaimsDownloader(period_start=period_start, period_end=period_end,
                                                    fills_and_reversals=FillsAndReversals.FILLS,
                                                    fill_status=FillStatus.FILLED, chains=chain_enum,
                                                    partners=Partners.ALL,
                                                    print_sql_flag=True)
    raw_claims_df = claims_src.pull_data()
    claims_df = preprocess_claims_df(raw_claims_df, period_start,
                                     period_end)  # apply all necessary filters and conditions for specific chain

    src = SourceDownloader(tables=[HistoricalData.PHARMACY_S3, HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3,
                                   MediSpan.NAME_S3, MediSpan.NDC_S3, Reporting.CHAIN_RATES_RS], get_modified_dfs=True)
    source_dict = src.generate_sources_dictionary()

    claims_processing_fees_src = claims_processing_fees.ClaimsProcFeesSource(period_start, period_end, chain_name)
    source_dict['claim_processing_fees'] = claims_processing_fees_src.downloader.pull_claim_processing_fees(
        sql_text=claims_processing_fees_src.invoice_sql)

    ### PROCESS TRANSACTIONS ###
    # build transactions dataframe
    transaction_raw_df = transform_claims.build_default_transactions_dataset(claims_df, source_dict, chain_name,
                                                                             chain_class, period_start, period_end)

    ### VALIDATE PRICE BASIS ###
    # need this validation in order to ensure that we don't blindly reconcile agains reconciliation_price_basis
    # if any descripancies found - we will receive notification in #temp_invoices slack channel and decide is it expected or not
    validation_dict = utils.validate_price_basis(transaction_raw_df, period_start, period_end, invoice_bucket,
                                                 chain_name)

    transaction_results = transaction_raw_df.copy()
    transaction_final_df = chain_class._select_fields_for_exporting(transactions_df=transaction_results)

    transaction_file_name = utils.transactions_file_name(chain_name=chain_name.upper(), report_type=period)

    log.info('processing: ABR/AGR/ANR info')
    ger_results = utils.calculate_ger_and_ber_metrics(transaction_raw_df)
    anr_df = calculate_anr_metric(ger_results)
    summary_results = utils.create_report_dict(ger_results, dispensing_fee_flag=True)
    summary_results = calculate_afer_metrics(ger_results, summary_results)

    summary_results['NADAC Report'] = pd.concat([summary_results['NADAC Report'], anr_df])

    log.info('processing: quarterly summary')
    date_period = f'Period: {period_start.strftime("%Y-%m-%d")} - {period_end.strftime("%Y-%m-%d")}'
    summary_bytes = utils.write_quarterly_reconciliation_to_excel(summary_results, date_period, dispensing_fee_report_flag=True, afer_flag=True)
    summary_results = pd.concat(summary_results.values(), axis=0, ignore_index=True)

    period_str = 'Quarterly' if period == 'quarter' else 'Annual'

    to_send_dict = {
        'summary_report': {
            'df': summary_results,
            'bytes': summary_bytes,
            'file_name': f"Hippo_{chain_name.capitalize()}_{period_str}_Summary_" + dt.today().strftime("%m%d%Y") + ".xlsx"
        },
    }
    for name, bytes in to_send_dict.items():
        if bytes != None:
            iu.send_file(bytes['df'], chain_name, bytes['file_name'], bytes['bytes'], slack_channel, slack_bot_token,
                         invoice_bucket, hsdk_env=hsdk_env, **kwargs)
        else:
            log.info("no data for file" + name)

    if validation_dict['dataframe'].empty == False:
        utils.send_text_message_to_slack(kwargs.get('temp_slack_channel'), slack_bot_token, validation_dict['message'])

    iu.create_and_send_file("transactions", transaction_final_df, transaction_file_name, slack_channel,
                            slack_bot_token, invoice_bucket, chain=chain_name.capitalize(), hsdk_env=hsdk_env, **kwargs)


def preprocess_claims_df(raw_claims_df, period_start, period_end):
    claims_df = sources_claims.preprocess_claims(raw_claims_df, partners=True)
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df,
                                                                chain_class.BIN_NUMBERS)  # filter dataframe by bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor(
        'D') >= chain_class.CONTRACT_START_DATE]  # go live for Mejer contract 2025-01-01
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)
    return claims_df

def calculate_anr_metric(grouped_df):
    filtered_df = grouped_df[grouped_df['reconciliation_price_basis'] == 'NADAC']
    # Calculate specific metric, ANR
    #
    # ((A+B)-C)/D
    #
    # A is the column ingredient_cost_paid_resp
    # B is full_cost
    # C is dispensing_fee_paid_resp
    # and D Total claims
    anr_value = (
            (filtered_df['ingredient_cost_paid_resp'].sum() + filtered_df['dispensing_fee_paid_resp'].sum() )
            - filtered_df['full_cost'].sum()
          ) / filtered_df['Total claims'].sum()

    index = ['Actual ANR (merge_cells)', 'Contracted ANR (merge_cells)']

    columns = ['Brand', 'Generic', 'Total Variant Amount']
    anr_rate = pd.DataFrame(columns=columns, index=index)
    contracted_rate = float(chain_class.TARGET_ANR)

    anr_rate.loc['Actual ANR (merge_cells)'] = [anr_value] * len(columns)
    anr_rate.loc['Contracted ANR (merge_cells)'] = [contracted_rate] * len(columns)

    return anr_rate

def calculate_afer_metrics(grouped_df, report_dict):
    # process AFER block
    afer_value = grouped_df['total_paid_response'].sum() / grouped_df['Total claims'].sum()
    target_afer = Decimal(chain_class.TARGET_AFER)
    afer_diff = afer_value - target_afer
    report_dict['AFER'] = pd.DataFrame({'AFER': [afer_value]})
    report_dict['Target AFER'] = pd.DataFrame({'Target AFER': [target_afer]})
    report_dict['AFER Diff'] = pd.DataFrame({'AFER Diff': [afer_diff]})

    return report_dict
