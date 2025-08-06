#!/usr/bin/env python3

from sources import claims as sources_claims
from transform import utils
from transform.cvs import cvs_bi_weekly_invoices, cvs_bi_weekly_transactions
from hippo.sources.download_sources import SourceDownloader, HistoricalData, MediSpan
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, FillStatus
from datetime import timedelta, datetime

from . import invoice_utils as iu

VENDOR_NUMBER = "0200020DAT"
CHAIN = "CVS"
ADDR_LINE_2 = "Attn: MC 0287"
ADDR_LINE_3 = "200 Highland Corporate Dr."
ADDR_LINE_4 = "Cumberland, RI  02864"
ADDR_LINE_5 = ""

def run(invoice_bucket, temp_slack_channel, slack_bot_token, hsdk_env, **kwargs):
    if hsdk_env != 'dev': # temprory way to avoid running script in production and sending invoice twice to slack #temp-invoices channel
        return
    period_start, period_end = utils.process_invoice_dates(period_flag='bi-weekly')
    claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS_AND_REVESALS, fill_status=FillStatus.FILLED, chains=Chains.CVS)
    raw_claims_df = claims_src.pull_data()
    claims_df = sources_claims.preprocess_claims(raw_claims_df)
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)
    src = SourceDownloader(tables=[HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3, MediSpan.NAME_S3, MediSpan.NDC_S3], get_modified_dfs=True) 
    source_dict = src.generate_sources_dictionary()
    # calculate invoices and transactions
    invoice_results = cvs_bi_weekly_invoices.transform_cvs_invoice_data(claims_df, source_dict['ndcs_v2'], period_start, period_end)
    transaction_results = cvs_bi_weekly_transactions.transform_cvs_transactions_data(claims_df, source_dict, period_start, period_end)
    transaction_file_name = utils.transactions_file_name(chain_name=CHAIN)

    iu.validate_results(invoice_results, transaction_results)

    iu.create_and_send_file_python("invoice", invoice_results, '', temp_slack_channel, 
                            slack_bot_token, invoice_bucket, 
                            chain = CHAIN,
                            vendor_number = VENDOR_NUMBER,
                            addr_line_1 = CHAIN,
                            addr_line_2 = ADDR_LINE_2,
                            addr_line_3 = ADDR_LINE_3,
                            addr_line_4 = ADDR_LINE_4,
                            addr_line_5 = ADDR_LINE_5,)

    iu.create_and_send_file_python("transactions", transaction_results, transaction_file_name, temp_slack_channel, 
                            slack_bot_token, invoice_bucket, chain=CHAIN)
