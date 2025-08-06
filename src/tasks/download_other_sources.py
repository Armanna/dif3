import pandas as pd
from io import BytesIO
from transform import utils
from hippo.exporters import Registry
from hippo.exporters import s3 as s3_exporter
from hippo.sources.download_sources import SourceDownloader, HistoricalData, MediSpan, Reporting, HistoricalDataPbmHippo as hd_pbm_hippo
from hippo.sources.s3 import S3Downloader
from hippo import s3 as hippo_s3

from hippo import logger

log = logger.getLogger('download_other_sources.py')

def run(chain_name, period, manual_date_string, invoice_bucket, s3_exporter_enabled, **kwargs):
    
    log.info(f"Downloading sources for the chain: \n {chain_name}")
    log.info(f"Period type: \n {period}")
    temp_main_prefix = f"temp/sources/{chain_name}/{period}"
    temp_chain_specific_prefix = f"temp/sources/specific/{chain_name}/{period}"
    temp_claims_prefix = f"temp/claims/{chain_name}/{period}"

    claims_df = S3Downloader(invoice_bucket, temp_claims_prefix, 'claims.parquet').pull_parquet()[0]
    min_claims_date_of_service = claims_df.claim_date_of_service.min() if chain_name != 'walmart' else pd.to_datetime('2024-01-03')
    max_claims_date_of_service = claims_df.claim_date_of_service.max()

    # download claims from the DB based on the date of current period not current day
    main_src, chain_specific_src = define_soruces_src(chain_name, period)
    source_dict = main_src.generate_sources_dictionary()
    if chain_specific_src:
        chain_specific_dict = chain_specific_src.generate_sources_dictionary()
        for name, df in chain_specific_dict.items():
            if 'chain_name' in df.columns.tolist() and name != 'pharmacy':
                log.info(f'Filter {name} by chain name df[df["chain_name"] == {chain_name}]')
                size_before = df.shape[0]
                df = df[df['chain_name'] == chain_name]
                log.info(f'Number of rows before filtering: {size_before}. Number of rows after filtering: {df.shape[0]}')
            if 'valid_to' in df.columns.tolist():
                log.info(f'Filter {name} by valid_to df[df["valid_to"] > {min_claims_date_of_service - pd.Timedelta(days=1)}]')
                size_before = df.shape[0]
                df = df[df['valid_to'] > min_claims_date_of_service - pd.Timedelta(days=1)]
                log.info(f'Number of rows before filtering: {size_before}. Number of rows after filtering: {df.shape[0]}')
            if 'valid_from' in df.columns.tolist():
                log.info(f'Filter {name} by valid_from df[df["valid_from"] < {max_claims_date_of_service + pd.Timedelta(days=1)}]')
                size_before = df.shape[0]
                df = df[df['valid_from'] < max_claims_date_of_service + pd.Timedelta(days=1)]
                log.info(f'Number of rows before filtering: {size_before}. Number of rows after filtering: {df.shape[0]}')
            buffer = BytesIO()
            df.to_parquet(buffer, compression='snappy', engine='pyarrow')
            log.info(f"Saving data in {invoice_bucket}/{temp_chain_specific_prefix}")
            export = Registry()\
                    .add_exporter('s3', s3_exporter.S3Exporter(
                        invoice_bucket,
                        temp_chain_specific_prefix,
                        enabled=s3_exporter_enabled,
                    ))

            export.emit(f'{name}.parquet', buffer)

    for name, df in source_dict.items():
        if 'chain_name' in df.columns.tolist() and name != 'pharmacy':
            log.info(f'Filter {name} by chain name df[df["chain_name"] == {chain_name}')
            size_before = df.shape[0]
            df = df[df['chain_name'] == chain_name]
            log.info(f'Number of rows before filtering: {size_before}. Number of rows after filtering: {df.shape[0]}')
        if 'valid_to' in df.columns.tolist():
            log.info(f'Filter {name} by valid_to df[df["valid_to"] > {min_claims_date_of_service - pd.Timedelta(days=1)}]')
            size_before = df.shape[0]
            df = df[df['valid_to'] > min_claims_date_of_service - pd.Timedelta(days=1)]
            log.info(f'Number of rows before filtering: {size_before}. Number of rows after filtering: {df.shape[0]}')
        if 'valid_from' in df.columns.tolist():
            log.info(f'Filter {name} by valid_from df[df["valid_from"] < {max_claims_date_of_service + pd.Timedelta(days=1)}]')
            size_before = df.shape[0]
            df = df[df['valid_from'] < max_claims_date_of_service + pd.Timedelta(days=1)]
            log.info(f'Number of rows before filtering: {size_before}. Number of rows after filtering: {df.shape[0]}')
        buffer = BytesIO()
        df.to_parquet(buffer, compression='snappy', engine='pyarrow')
        log.info(f"Saving data in {invoice_bucket}/{temp_main_prefix}")
        export = Registry()\
                .add_exporter('s3', s3_exporter.S3Exporter(
                    invoice_bucket,
                    temp_main_prefix,
                    enabled=s3_exporter_enabled,
                ))

        export.emit(f'{name}.parquet', buffer)


def define_soruces_src(chain_name, period):
    if chain_name == 'cvs' and period != 'bi-weekly':
        main_src = SourceDownloader(tables=[MediSpan.NAME_S3, MediSpan.NDC_S3, Reporting.CHAIN_RATES_RS, HistoricalData.PHARMACY_S3], get_modified_dfs=True) 
        chain_specific_src = SourceDownloader(tables=[HistoricalData.NDC_V2_S3, HistoricalData.NDC_COST_V2_S3, HistoricalData.EXCLUDED_NDCS_S3, hd_pbm_hippo.CVS_SPECIALTY_LIST_S3, hd_pbm_hippo.CVS_TPDT_MAC_HISTORY], get_modified_dfs=True) 
    elif chain_name == 'cvs' and period in ['bi-weekly']:
        main_src = SourceDownloader(tables=[HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3, MediSpan.NAME_S3, MediSpan.NDC_S3], get_modified_dfs=True) 
        chain_specific_src = None
    elif chain_name == 'walmart':
        main_src = SourceDownloader(tables=[HistoricalData.PHARMACY_S3, MediSpan.NAME_S3, MediSpan.NDC_S3, Reporting.CHAIN_RATES_RS], get_modified_dfs=True)
        chain_specific_src = SourceDownloader(tables=[hd_pbm_hippo.WALMART_EXCLUDED_NDCS, hd_pbm_hippo.WALMART_FOUR_DOLLAR_NDCS, HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3], get_modified_dfs=True)
    elif chain_name == 'albertsons':
        main_src = SourceDownloader(tables=[HistoricalData.PHARMACY_S3, HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3, MediSpan.NAME_S3, MediSpan.NDC_S3, Reporting.CHAIN_RATES_RS], get_modified_dfs=True) 
        chain_specific_src = None
    else:
        main_src = None
        chain_specific_src = None
    return main_src, chain_specific_src
        

def load_parquet_files_to_dict(s3_bucket, s3_prefix):
    """
    this function scan s3_bucket/s3_prefix and save all .parquet files in the dictionary 
    where file name without extention is a key and python dataframe is a value
    """
    keys = hippo_s3.dir(bucket_name=s3_bucket, prefix=s3_prefix)

    if not keys:
        raise ValueError(f"No files found in bucket '{s3_bucket}' with prefix '{s3_prefix}'")

    files_dict = {}
    
    for file_key in keys:
        if file_key.endswith('.parquet'):
            file_name = file_key.split('/')[-1]
            log.info(f"Processing file: {file_name}")
            parquet_data = S3Downloader(s3_bucket, prefix=s3_prefix, name=file_name).pull_parquet()[0]
            file_name_without_extention = file_name.split('.')[0]
            files_dict[file_name_without_extention] = parquet_data
    
    return files_dict
