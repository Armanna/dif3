#!/usr/bin/env python3


from sources import redshift
from sources import reportingdates as rd
from . import invoice_utils as iu

from datetime import datetime


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()

    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")

    combinations = [
        {"bin": "019901", "partner": ""},
        {"bin": "019876", "partner": ""},
        {"bin": "019876", "partner": "webmd"}
    ]

    for combination in combinations:
        print("processing: yearly summary")
        summary_results = redshift_src.pull(
            "sources/CVS-summary.sql", params={ "bin": combination['bin'],
                                                "partner": combination['partner'],
                                                "period_start": rd.first_day_of_previous_year(),
                                                "period_end": rd.last_day_of_previous_year()})
        summary_file_name = "Hippo_CVS_Cash_Card_Yearly_Summary_"  + combination['bin'] + "_" + combination['partner'] + "_" + invoice_date + ".xlsx"

        iu.create_and_send_file("summary", summary_results, summary_file_name, slack_channel,
                            slack_bot_token, invoice_bucket, chain="CVS", hsdk_env=hsdk_env, **kwargs)
