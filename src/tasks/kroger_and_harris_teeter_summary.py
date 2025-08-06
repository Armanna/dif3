#!/usr/bin/env python3
import os

import pandas as pd
from datetime import datetime as dt, timedelta
from sources import claims as sources_claims
from transform import claims as transform_claims, draw_invoice
from transform import utils
from transform.kroger_and_harris_teeter import summary
from hippo.sources.download_sources import SourceDownloader, HistoricalData, MediSpan, HistoricalDataPbmHippo
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, FillStatus, BasisOfReimbursment
from sources import pharmacies

from . import invoice_utils as iu

chain = pharmacies.KrogerAndHarrisTeeter

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, chain_name, period, **kwargs):

    period_start, period_end = utils.process_invoice_dates(period_flag=period)

    ytd_period_start = max(period_start.replace(month=1, day=1), chain.CONTRACT_START_DATE)

    # manually handle exclusion for May 2024 because we started processing Kroger claims within direct deal at 15th of May while standard invoicing periods are 1st-15th, 16th-the_end_of_the_month_date; so instead of 2024-05-16 - 2024-05-31 we need to process 2024-05-15 - 2024-05-31
    period_start = max(period_start, chain.CONTRACT_START_DATE)

    if chain_name == 'kroger':
        chain_enum = Chains.KROGER
    elif chain_name == 'harris_teeter':
        chain_enum = Chains.HARRIS_TEETER

    # downloads and process claims
    claims_src = claims_downloader.ClaimsDownloader(period_start = ytd_period_start, period_end = period_end,
        fills_and_reversals = FillsAndReversals.FILLS, basis_of_reimbursment=BasisOfReimbursment.NON_UNC,
                                    fill_status=FillStatus.FILLED, chains=chain_enum, print_sql_flag=True)
    raw_claims_df = claims_src.pull_data()

    claims_df = preprocess_claims_df(raw_claims_df, period_start, period_end) # apply all necessary filters and conditions for specific chain
    # downloads source files
    src = SourceDownloader(tables=[HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3, MediSpan.NAME_S3,
                                   MediSpan.NDC_S3, HistoricalDataPbmHippo.KROGER_MAC_CS_2_HISTORY,
                                   HistoricalData.DRUG_NDCS_WITH_RXNORM, HistoricalDataPbmHippo.KROGER_MAC_CS_3_4_5_HISTORY], get_modified_dfs=True)
    source_dict = src.generate_sources_dictionary()

                                        ### PROCESS SUMMARY ###
    summary_bytes, summary_dataframe = summary.transform_data(claims_df, source_dict, period_start, period_end, period)
    summary_file_name = f'Hippo_{chain_name.capitalize()}_{period.capitalize()}ly_Summary_{(period_end + timedelta(days=1)).strftime("%Y-%m-%d")}.xlsx'
        
    iu.send_file(summary_dataframe, chain_name.upper(), summary_file_name, summary_bytes, slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)

def preprocess_claims_df(raw_claims_df, period_start, period_end):
    claims_df = sources_claims.preprocess_claims(raw_claims_df)
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, chain.BIN_NUMBERS, chain.CHAIN_CODES)        # filter dataframe by chan_code, bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= chain.CONTRACT_START_DATE]                # go live for Kroger contract 2024-05-15
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)
    return claims_df
