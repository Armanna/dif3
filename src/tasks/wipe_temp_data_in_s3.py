from hippo.exporters import s3 as s3_exporter
from hippo import logger

log = logger.getLogger('wipe_temp_data_in_s3.py')

def run(invoice_bucket, chain_name, period, **kwargs):
    claims_prefix=f'temp/claims/{chain_name}/{period}/'
    sources_prefix=f'temp/sources/{chain_name}/{period}/'
    sources_specific_prefix=f'temp/sources/specific/{chain_name}/{period}/'

    log.info(f"\nDeleting data in:")
    log.info(f"Bucket: \n {invoice_bucket}")
    log.info(f"Prefixes: \n {claims_prefix}, {sources_prefix}, {sources_specific_prefix}")

    for prefix in [claims_prefix, sources_prefix, sources_specific_prefix]:
        wipe = s3_exporter.S3Exporter(invoice_bucket, prefix)
        wipe._wipe()
