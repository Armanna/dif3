from sources.redshift import InvoicesRedshiftDownloader
from hippo import logger

log = logger.getLogger('claim_processing_fees')

class ClaimsProcFeesSource:
    def __init__(self, period_start, period_end, chain_name):
        log.info('Using default Redshift credentials: Downloading reporting.claim_processing_fees data')
        self.downloader = InvoicesRedshiftDownloader()
        self.invoice_sql = f"""
            SELECT 
                valid_from::timestamp,
                rx_id,
                pbm_fee::decimal(10,2) / 100::decimal(3,0) as pbm_fee,
                erx_fee::decimal(10,2) / 100::decimal(3,0) as erx_fee,
                processor_fee::decimal(10,2) / 100::decimal(3,0) as processor_fee
            FROM 
                reporting.claim_processing_fees
            WHERE
                rx_id||valid_from::timestamp IN (
                    SELECT 
                        c.rx_id || c.valid_from::timestamp 
                    FROM 
                        reporting.claims c
                    LEFT JOIN 
                        historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from :: timestamp >= ph.valid_from :: timestamp AND c.valid_from :: timestamp < ph.valid_to :: timestamp
                    WHERE 
                        ph.chain_name = '{chain_name}'
                        AND (
                            (c.valid_to::date >= '{period_start}'::date 
                             and c.valid_to::date <= '{period_end}'::date 
                             and c.fill_status = 'filled')
                            OR
                            (c.valid_from::date >= '{period_start}'::date 
                             and c.valid_from::date <= '{period_end}'::date 
                             and c.valid_to::date > '{period_end}'::date 
                             and c.fill_status = 'filled')
                        )
                )
        """
