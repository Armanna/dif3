import pandas as pd
from hippo.sources.redshift import RedshiftDownloader
from transform import utils

class Redshift:
    def __init__(self):
        self.downloader = RedshiftDownloader()

    def _query_db(self, sql_file, params):
        file = open(sql_file, 'r')
        sql = file.read()
        data = self.downloader.pull(sql.format(**params))

        print(data.head(10))
        sql_data_size = len(data.index)

        assert sql_data_size > 0, "query results are empty"

        return data

    def pull(self, sql_file, params={}):
        return self._query_db(sql_file, params)

class InvoicesRedshiftDownloader(RedshiftDownloader):

    def pull_claim_processing_fees(self, sql_text):
        claim_processing_fees_df = self.pull(sql_text)
        claim_processing_fees_df['valid_from'] = claim_processing_fees_df['valid_from'].astype('datetime64[ns]')
        claim_processing_fees_df = utils.cast_columns_to_decimal(claim_processing_fees_df, column_names=['erx_fee','processor_fee','pbm_fee'], fillna_flag=True)
        return claim_processing_fees_df
