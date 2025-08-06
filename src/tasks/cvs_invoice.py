#!/usr/bin/env python3

import io
from sources import redshift
from . import invoice_utils as iu

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()
    
    invoice_results = redshift_src.pull("sources/CVS-invoice.sql")

    transaction_results, transaction_file_name = iu.extract_transactions(redshift_src, 'CVS')  

    iu.validate_results(invoice_results, transaction_results)

    vendor_number = "0200020DAT"
    iu.create_and_send_file("invoice", invoice_results, '', slack_channel, 
                            slack_bot_token, invoice_bucket, 
                            chain = "CVS",
                            vendor_number = vendor_number,
                            addr_line_1 = "CVS",
                            addr_line_2 = "Attn: MC 0287",
                            addr_line_3 = "200 Highland Corporate Dr.",
                            addr_line_4 = "Cumberland, RI  02864",
                            addr_line_5 = "", hsdk_env=hsdk_env, **kwargs)

    if not transaction_results.empty: # save as .csv in s3 bucket and text to #ivoices channel in slack
        file_bytes = io.BytesIO()
        transaction_results.to_csv(file_bytes, index=False, encoding='utf-8')
        iu.send_file(transaction_results, "CVS", transaction_file_name.replace(".xlsx", ".csv"), file_bytes,
                    slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)     
