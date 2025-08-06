#!/usr/bin/env python3

from os import environ, remove
import os
import sys

from sources import redshift
from . import invoice_utils as iu

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()
    
    invoice_results = redshift_src.pull("sources/Walgreens-invoice.sql")

    transaction_results, transaction_file_name = iu.extract_transactions(redshift_src, 'Walgreens')  

    iu.validate_results(invoice_results, transaction_results)

    vendor_number = ""
    iu.create_and_send_file("invoice", invoice_results, '', slack_channel, 
                            slack_bot_token, invoice_bucket, 
                            chain = "Walgreens",
                            vendor_number = vendor_number,
                            addr_line_1 = "Walgreen Co.",
                            addr_line_2 = "102 Wilmot Rd",
                            addr_line_3 = "MSL#1213",
                            addr_line_4 = "Deerfield IL",
                            addr_line_5 = "60015", hsdk_env=hsdk_env, **kwargs)

    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain="Walgreens", hsdk_env=hsdk_env, **kwargs)
    
