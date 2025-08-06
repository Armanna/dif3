#!/usr/bin/env python3

import io

from sources import redshift
from sources import reportingdates as rd
from . import invoice_utils as iu

import pandas as pd
from datetime import datetime


def validate_results(summary_results, transaction_results):

    summary_totals = summary_results.sum(axis=0)

    fill_minimums = transaction_results[transaction_results['reversal indicator'] == 'B1'].min(
        axis=0)
    reversal_minimums = transaction_results[transaction_results['reversal indicator'] == 'B2'].min(
        axis=0)
    reversal_maximums = transaction_results[transaction_results['reversal indicator'] == 'B2'].max(
        axis=0)
    transaction_maximums = transaction_results.max(axis=0)

    assert summary_totals["claim count"] == \
        len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index) \
        - len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index), \
        "Total claims in summary do not match fills minus reversals.\n" + \
        "\tTotal claims count: " + str(summary_totals["claim count"]) + "\n" + \
        "\tTotal fills: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index)) + "\n" + \
        "\tTotal reversals: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index)) + "\n" +  \
        "\tCalculated fill count: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index)
                                          - len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index))

    assert datetime.strptime(fill_minimums["fill date"], '%Y-%m-%d') \
        >= datetime.strptime(summary_results.iloc[0]["period_start"], '%m/%d/%Y'), \
        "All fills do not start after the invoice start date. \n" \
        "\tEarliest transaction: " + str(fill_minimums["fill date"]) + "\n" + \
        "\tSummary start date: " + str(summary_results.iloc[0]["period_start"])

    # first quarter so no reversals
    # assert datetime.strptime(reversal_minimums["reversal date"],'%Y-%m-%d') \
    #              >= datetime.strptime(summary_results.iloc[0]["period_start"],'%m/%d/%Y')  , \
    #     "All transaction were not done after the invoice start date\n" \
    #         "\tEarliest transaction date: " + str(reversal_minimums["reversal date"]) + "\n" + \
    #         "\Summary start date: " +str(summary_results.iloc[0]["period_start"] )

    # assert datetime.strptime(reversal_maximums["fill date"],'%Y-%m-%d') \
    #              < datetime.strptime(invoice_results.iloc[0]["period_start"],'%m/%d/%Y'), \
    #     "All reversals were not for fills made before the invoice start date\n" \
    #         "\tlatest transaction date: " + str(reversal_maximums["fill date"]) + "\n" + \
    #         "\tInvoice start date: " +str(invoice_results.iloc[0]["period_start"] )

    ingredient_total = summary_results["total ingredient cost"].sum(axis=0)
    calculated_ingredient_cost = transaction_results["ingredient cost paid"].sum(
        axis=0)
    assert abs(ingredient_total - calculated_ingredient_cost) < 0.01, \
        "Ingredient cost does not match transactions ingredient costs\n" \
        "\tTotal ingredient cost: " + str(ingredient_total) + "\n" + \
        "\tTotal transaction ingredient cost: " + \
        str(calculated_ingredient_cost)

    administration_fee_total = summary_results["total administration fee"].sum(
        axis=0)
    calculated_administration_fee = round(
        transaction_results["administration fee"].sum(axis=0), 2)
    assert abs(administration_fee_total - calculated_administration_fee) < 0.01, \
        "Admin fee does not match sum of fills and reversals\n" \
        "\tTotal admin fee: " + str(administration_fee_total) + "\n" + \
        "\tTotal transactions admin fee: " + \
        str(calculated_administration_fee)

    dollar_variance = summary_results["dollar variance"].sum(axis=0)
    calculated_dollar_variance = round(
        transaction_results["dollar variance"].sum(axis=0), 2)
    assert abs(dollar_variance - calculated_dollar_variance) < 150, \
        "Variance does not match sum of fills and reversals\n" \
        "\tTotal variance: " + str(dollar_variance) + "\n" + \
        "\tTotal transactions variance: " + str(calculated_dollar_variance)

def add_exempt_generics(period_start, period_end, summary_results):
    if 'exempt generic' in summary_results['drug type'].values:
        return summary_results
    else:
        exempt_generic_row = pd.DataFrame({'period_start': period_start, 'period_end': period_end, \
                                           'start_date': period_start, 'end_date': period_end, \
                                           'days_supply': 'Any', 'drug type': 'exempt generic', \
                                           'basis_of_reimbursement_source': '','claim count': 0, \
                                           'total ingredient cost': 0, 'total administration fee': 0, \
                                           'total full cost': 0, 'actual effective rate':  '', \
                                           'target effective rate': '', 'effective rate variance': 0, \
                                           'dollar variance': 0}, index=[len(summary_results)])
        return pd.concat([summary_results, exempt_generic_row], ignore_index=True)
        

def run(bin, partner, invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()

    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")

    print("processing: summary")
    if bin != '019901':
        summary_results = redshift_src.pull(
            "sources/CVS-quarterly-summary.sql", params={  "bin": bin,
                                                            "partner": partner,
                                                            "period_start": rd.first_day_of_previous_quarter(),
                                                            "period_end": rd.last_day_of_previous_quarter()})

        summary_results = add_exempt_generics(rd.first_day_of_previous_quarter(), rd.last_day_of_previous_quarter(), summary_results)
    else:
        summary_results = redshift_src.pull(
            "sources/CVS-quarterly-summary-tpdt.sql", params={
                                                            "period_start": rd.first_day_of_previous_quarter(),
                                                            "period_end": rd.last_day_of_previous_quarter()
                                                            }
                                            )


    summary_file_name = "Hippo_CVS_Quarterly_Summary_" + bin + "_" + partner + "_" + invoice_date + ".xlsx"

    print("processing: transactions")
    transaction_results_df = redshift_src.pull(
        "sources/CVS-quarterly-transactions.sql", params={"bin": bin,
                                                          "partner": partner})
    columns_to_drop = ['fill_date', 'chain_name', 'price_basis']
    transaction_results_df = transaction_results_df.drop(columns=columns_to_drop)
    transaction_file_name = "Hippo_CVS_Quarterly_Transactions_" + bin + "_" + partner + "_" + invoice_date + ".csv"
    
    validate_results(summary_results.rename(columns={"ic dollar variance": "dollar variance"}), transaction_results_df)

    if not transaction_results_df.empty: # save as .csv in s3 bucket and text to #ivoices channel in slack because file is too big for excel format and for sending to slack
        file_bytes = io.BytesIO()
        transaction_results_df.to_csv(file_bytes, index=False, encoding='utf-8')
        iu.send_file(transaction_results_df, "CSV", transaction_file_name, file_bytes,
                    slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)

    iu.create_and_send_file("summary", summary_results, summary_file_name, slack_channel,
                            slack_bot_token, invoice_bucket, chain="CVS", hsdk_env=hsdk_env, **kwargs)
