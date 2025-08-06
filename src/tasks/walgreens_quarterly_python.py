#!/usr/bin/env python3

from sources import claims as sources_claims
from transform import utils
from transform.walgreens import walgreens_utils, walgreens_claims, walgreens_quarterly_summary
from hippo.sources.download_sources import Reporting, SourceDownloader, HistoricalData
from hippo.sources import claims_downloader
from hippo.sources.claims import Chains, FillStatus, BasisOfReimbursment

from datetime import datetime

from . import invoice_utils as iu

def run(invoice_bucket, temp_slack_channel, slack_bot_token, hsdk_env, **kwargs):
    if hsdk_env != 'dev': # temprory way to avoid running script in production and sending invoice twice to slack #temp-invoices channel
        return
    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")

    summary_file_name = "Hippo_Walgreens_Quarterly_Summary_" + invoice_date + ".xlsx" 

    period_start, period_end = utils.process_invoice_dates(period_flag='quarter')
    walgreens_period_start, walgreens_period_end = walgreens_utils.convert_to_utc_plus_6(period_start, period_end)
    claims_src = claims_downloader.ClaimsDownloader(period_start = walgreens_period_start, period_end = walgreens_period_end, chains=Chains.WALGREENS, fill_status = FillStatus.FILLED, basis_of_reimbursment=BasisOfReimbursment.AWP)

    raw_claims_df = claims_src.pull_data()
    # convert_dates_to_cst6cdt=True because we need to convert valid_from and valud_to columns to cst6cdt time zone
    claims_df = sources_claims.preprocess_claims(raw_claims_df, convert_dates_to_cst6cdt=True)
    claims_df = walgreens_claims.filter_walgreens_claims(claims_df, period_start, period_end)
    src = SourceDownloader(tables=[Reporting.CHAIN_RATES_RS, HistoricalData.PHARMACY_S3, HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3], get_modified_dfs=True)
    source_dict = src.generate_sources_dictionary() 
    # calculate invoices and transactions
    summary_results = walgreens_quarterly_summary.transform_walgreens_summary_data(claims_df, source_dict['pharmacy'], source_dict['ndcs_v2'], source_dict['chain_rates'], source_dict['ndc_costs_v2'], period_start, period_end)

    iu.create_and_send_file_python("summary", summary_results, summary_file_name, temp_slack_channel,
                    slack_bot_token, invoice_bucket, chain="Walgreens")
