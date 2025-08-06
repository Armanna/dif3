#!/usr/bin/env python3

from sources import claims as sources_claims
from transform import utils
from transform.walgreens import walgreens_monthly_invoices, walgreens_utils, walgreens_monthly_transactions, walgreens_claims
from hippo.sources.download_sources import SourceDownloader, HistoricalData, MediSpan
from hippo.sources import claims_downloader
from hippo.sources.claims import Chains, FillStatus


from . import invoice_utils as iu

VENDOR_NUMBER = ""
CHAIN = "Walgreens"
ADDR_LINE_1 = "Walgreen Co."
ADDR_LINE_2 = "102 Wilmot Rd"
ADDR_LINE_3 = "MSL#1213"
ADDR_LINE_4 = "Deerfield IL"
ADDR_LINE_5 = "60015"

def run(invoice_bucket, temp_slack_channel, slack_bot_token, hsdk_env, **kwargs):
    if hsdk_env != 'dev': # temprory way to avoid running script in production and sending invoice twice to slack #temp-invoices channel
        return
    period_start, period_end = utils.process_invoice_dates(period_flag='month')
    walgreens_period_start, walgreens_period_end = walgreens_utils.convert_to_utc_plus_6(period_start, period_end)
    claims_src = claims_downloader.ClaimsDownloader(period_start = walgreens_period_start, period_end = walgreens_period_end, chains=Chains.WALGREENS, fill_status = FillStatus.FILLED)

    raw_claims_df = claims_src.pull_data()
    # convert_dates_to_cst6cdt=True because we need to convert valid_from and valud_to columns to cst6cdt time zone
    claims_df = sources_claims.preprocess_claims(raw_claims_df, convert_dates_to_cst6cdt=True)
    claims_df = walgreens_claims.filter_walgreens_claims(claims_df, period_start, period_end)
    src = SourceDownloader(tables=[HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3, MediSpan.NAME_S3, MediSpan.NDC_S3], get_modified_dfs=True) 
    source_dict = src.generate_sources_dictionary() 
    # calculate invoices and transactions
    invoice_results = walgreens_monthly_invoices.transform_walgreens_invoice_data(claims_df, source_dict['ndcs_v2'], period_start, period_end)
    transaction_results = walgreens_monthly_transactions.transform_walgreens_transactions_data(claims_df, source_dict)
    transaction_file_name = utils.transactions_file_name(chain_name=CHAIN)

    iu.create_and_send_file_python("invoice", invoice_results, '', temp_slack_channel, 
                            slack_bot_token, invoice_bucket, 
                            chain = CHAIN,
                            vendor_number = VENDOR_NUMBER,
                            addr_line_1 = ADDR_LINE_1,
                            addr_line_2 = ADDR_LINE_2,
                            addr_line_3 = ADDR_LINE_3,
                            addr_line_4 = ADDR_LINE_4,
                            addr_line_5 = ADDR_LINE_5,)

    iu.create_and_send_file_python("transactions", transaction_results, transaction_file_name, temp_slack_channel, 
                            slack_bot_token, invoice_bucket, chain=CHAIN)
    
