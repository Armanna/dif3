import pandas as pd
from io import BytesIO
from datetime import datetime as dt
from transform import utils

from sources import claims as sources_claims
from hippo.exporters import Registry
from hippo.exporters import s3 as s3_exporter
from hippo import logger
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, BasisOfReimbursment, FillStatus, Partners

log = logger.getLogger('download_claims.py')

def run(chain_name, period, manual_date_string, invoice_bucket, s3_exporter_enabled, **kwargs):
    
    log.info(f"Downloading claims for the chain: \n {chain_name}")
    log.info(f"Period type: \n {period}")
    temp_prefix = f"temp/claims/{chain_name}/{period}"
    period_start, period_end = utils.process_invoice_dates(period_flag=period, manual_date_str=manual_date_string)
    period_start, period_end = handle_exclusions_for_chain_effective_date(chain_name, period_start, period_end, period)
    
    if chain_name == 'walmart':
        current_year = period_start.year
        if current_year != 2024:
            period_start = dt.strptime(f'{current_year}-01-01', "%Y-%m-%d")
        if current_year == 2024:
            period_start = dt.strptime(f'2024-09-03', "%Y-%m-%d")

    log.info(f"Downloading the data for the period: {period_start} - {period_end}.")

    claims_src = define_claims_src(chain_name=chain_name, period=period, period_start=period_start, period_end=period_end)
    # download claims from the DB based on the date of current period not current day
    raw_claims_df = claims_src.pull_data()
    claims_df = sources_claims.preprocess_claims(raw_claims_df, partners=True)
    buffer = BytesIO()
    claims_df.to_parquet(buffer, compression='snappy', engine='pyarrow')
    log.info(f"Saving data in {invoice_bucket}/{temp_prefix}")
    export = Registry()\
            .add_exporter('s3', s3_exporter.S3Exporter(
                invoice_bucket,
                temp_prefix,
                enabled=s3_exporter_enabled,
            ))

    export.emit('claims.parquet', buffer)


def define_claims_src(chain_name, period, period_start, period_end):
    if chain_name == 'cvs' and period != 'bi-weekly':
        claims_src = claims_downloader.ClaimsDownloader(period_end = period_end, period_start = period_start, fills_and_reversals = FillsAndReversals.CVS_FILLS, chains=Chains.CVS, fill_status=FillStatus.FILLED, basis_of_reimbursment=BasisOfReimbursment.NON_UNC, partners=Partners.ALL, print_sql_flag=True)
    elif chain_name == 'cvs' and period in ['bi-weekly']:
        claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS_AND_REVESALS, fill_status=FillStatus.FILLED, chains=Chains.CVS, print_sql_flag=True)
    elif chain_name == 'walmart':
        claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS, fill_status=FillStatus.FILLED, chains=Chains.WALMART, partners=Partners.ALL, print_sql_flag=True)
    elif chain_name == 'albertsons':
        claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS_AND_OUT_OF_MONTH_REVERSALS, fill_status=FillStatus.FILLED, chains=[Chains.ALEBRTSONS, Chains.OTHER], partners=Partners.ALL, print_sql_flag=True)
    else:
        claims_src = None
    return claims_src

def handle_exclusions_for_chain_effective_date(chain_name, period_start, period_end, period):
    
    # manually handle exclusion for April 2025 because we oficially started processing Albertsons claims within direct deal at 1st of April
    if chain_name == 'albertsons' and period == 'bi-weekly':
        period_start = pd.to_datetime('2025-04-01') if period_end == pd.to_datetime('2025-04-30') else period_start
    elif chain_name == 'albertsons' and period == 'quarter':
        # contract says that we must deliver the data from the beginning of the year or the Effective date (whichever is later) through the last day of the quarter
        period_start = pd.to_datetime(f'{period_end.year}-01-01') if period_end.year > 2025 else (pd.to_datetime('2025-04-01') if period_end.year == 2025 else period_start)
    else:
        pass        
        
    return period_start, period_end
    
