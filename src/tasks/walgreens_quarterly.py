#!/usr/bin/env python3

from os import environ, remove
import io

from sources import redshift
from . import invoice_utils as iu

import pandas as pd
from datetime import datetime

def validate_results(summary_results, transaction_results):
    
    summary_totals = summary_results.sum(axis=0)     

    fill_minimums = transaction_results[transaction_results['reversal indicator']=='B1'].min(axis=0)     
    transaction_maximums = transaction_results.max(axis=0)
    valid_report = True

    try:
        assert summary_totals["claim count"] == \
                len(transaction_results[transaction_results['reversal indicator']=='B1'].index) \
                    - len(transaction_results[transaction_results['reversal indicator']=='B2'].index), \
                "Total claims in summary do not match fills minus reversals.\n" + \
                    "\tTotal claims count: " + str(summary_totals["claim count"]) + "\n" + \
                    "\tTotal fills: " + str(len(transaction_results[transaction_results['reversal indicator']=='B1'].index)) + "\n" + \
                    "\tTotal reversals: " + str(len(transaction_results[transaction_results['reversal indicator']=='B2'].index)) + "\n" +  \
                    "\tCalculated fill count: " + str(len(transaction_results[transaction_results['reversal indicator']=='B1'].index) \
                        - len(transaction_results[transaction_results['reversal indicator']=='B2'].index))
    except AssertionError as e:
        print("AssertionError: " + str(e))
        print("Total claims in summary: " + str(summary_totals["claim count"]))
        print("Total fills: " + str(len(transaction_results[transaction_results['reversal indicator']=='B1'].index)))
        print("Total reversals: " + str(len(transaction_results[transaction_results['reversal indicator']=='B2'].index)))
        valid_report = False

    try:
        assert datetime.strptime(fill_minimums["fill date"],'%Y-%m-%d') \
                    >= datetime.strptime(summary_results.iloc[0]["period_start"],'%m/%d/%Y')  , \
                "All fills do not start after the invoice start date. \n" \
                    "\tEarliest transaction: " + str(fill_minimums["fill date"]) + "\n" + \
                    "\tSummary start date: " +str(summary_results.iloc[0]["period_start"] )
    except AssertionError as e:
        print("AssertionError: " + str(e))
        print("Earliest transaction fill date: " + str(fill_minimums["fill date"]))
        print("Summary start date: " + str(summary_results.iloc[0]["period_start"]))
        valid_report = False

    try:
        ingredient_total = summary_results["total ingredient cost paid"].sum(axis=0)
        calculated_ingredient_cost = transaction_results["ingredient cost paid"].sum(axis=0)
        assert abs(ingredient_total - calculated_ingredient_cost) < 0.01, \
            "Ingredient cost paid does not match transactions ingredient costs\n" \
                "\tTotal ingredient cost paid: " + str(ingredient_total) + "\n" + \
                "\tTotal transaction ingredient cost paid: " + str(calculated_ingredient_cost)
    except AssertionError as e:
        print("AssertionError: " + str(e))
        print("Ingredient cost total: " + str(ingredient_total))
        print("Calculated ingredient cost: " + str(calculated_ingredient_cost))
        valid_report = False

    drug_cost_total = summary_results["total drug cost"].sum(axis=0)
    calculated_drug_cost = transaction_results["drug cost"].sum(axis=0)
    try:
        assert abs(drug_cost_total - calculated_drug_cost) < 0.01, \
            "Drug cost does not match transactions ingredient costs\n" \
            "\tTotal drug cost: " + str(drug_cost_total) + "\n" + \
            "\tTotal transaction drug cost: " + str(calculated_drug_cost)
    except AssertionError as e:
        print("AssertionError: " + str(e))
        print("Drug cost total: " + str(drug_cost_total))
        print("Calculated drug cost: " + str(calculated_drug_cost))
        valid_report = False

    administration_fee_total = summary_results["total administration fee"].sum(axis=0)
    calculated_administration_fee = round(transaction_results["administration fee"].sum(axis=0),2)
    try:
        assert  abs(administration_fee_total - calculated_administration_fee) < 0.01, \
        "Admin fee does not match sum of fills and reversals\n" \
            "\tTotal admin fee: " + str(administration_fee_total) + "\n" + \
            "\tTotal transactions admin fee: " + str(calculated_administration_fee)
    except AssertionError as e:
        print("AssertionError: " + str(e))
        print("Administration fee total: " + str(administration_fee_total))
        print("Calculated administration fee: " + str(calculated_administration_fee))
        valid_report = False

    dfer_dollar_variance = summary_results["dfer dollar variance"].sum(axis=0)
    calculated_dfer_dollar_variance = round(transaction_results["dfer dollar variance"].sum(axis=0), 2)
    try:
        assert abs(dfer_dollar_variance - calculated_dfer_dollar_variance) < 0.01, \
        "DFER Dollar variance does not match sum of fills and reversals\n" \
        "\tSummary variance: " + str(dfer_dollar_variance) + "\n" + \
        "\tTotal transactions variance: " + str(calculated_dfer_dollar_variance)
    except AssertionError as e:
        print("AssertionError: " + str(e))
        print("DFER dollar variance total: " + str(dfer_dollar_variance))
        print("Calculated DFER dollar variance: " + str(calculated_dfer_dollar_variance))
        valid_report = False

    ic_dollar_variance = summary_results["ic rate dollar variance"].sum(axis=0)
    calculated_ic_dollar_variance = round(transaction_results["ic rate dollar variance"].sum(axis=0), 2)
    try:
        assert abs(ic_dollar_variance - calculated_ic_dollar_variance) < 400, \
        "IC Rate Dollar variance does not match sum of fills and reversals\n" \
        "\tSummary variance: " + str(ic_dollar_variance) + "\n" + \
        "\tTotal transactions variance: " + str(calculated_ic_dollar_variance)
    except AssertionError as e:
        print("AssertionError: " + str(e))
        print("IC dollar variance total: " + str(ic_dollar_variance))
        print("Calculated IC dollar variance: " + str(calculated_ic_dollar_variance))
        valid_report = False

    total_dollar_variance = summary_results["total dollar variance"].sum(axis=0)
    calculated_total_dollar_variance = round(transaction_results["total dollar variance"].sum(axis=0),2)
    try:
        assert  abs(total_dollar_variance - calculated_total_dollar_variance) < 400, \
            "Total Dollar variance does not match sum of fills and reversals\n" \
                "\tSummary variance: " + str(total_dollar_variance) + "\n" + \
                "\tTotal transactions variance: " + str(calculated_total_dollar_variance)
    except AssertionError as e:
        print("AssertionError: " + str(e))
        print("Total dollar variance total: " + str(total_dollar_variance))
        print("Calculated total dollar variance: " + str(calculated_total_dollar_variance))
        valid_report = False

    if not valid_report:
        raise AssertionError("Validation failed for Walgreens quarterly report. Please check the logs for details.")

def validate_admin_results(summary_results, transaction_results):
    
    summary_totals = summary_results.sum(axis=0)     

    fill_minimums = transaction_results[transaction_results['prescription type']!='Reversal'].min(axis=0)     

    assert summary_totals["claim count"] == \
            len(transaction_results[transaction_results['prescription type']!='Reversal'].index) \
                - len(transaction_results[transaction_results['prescription type']=='Reversal'].index), \
            "Total claims in summary do not match fills minus reversals.\n" + \
                "\tTotal claims count: " + str(summary_totals["claim count"]) + "\n" + \
                "\tTotal fills: " + str(len(transaction_results[transaction_results['prescription type']!='Reversal'].index)) + "\n" + \
                "\tTotal reversals: " + str(len(transaction_results[transaction_results['prescription type']=='Reversal'].index)) + "\n" +  \
                "\tCalculated fill count: " + str(len(transaction_results[transaction_results['prescription type']!='Reversal'].index) \
                    - len(transaction_results[transaction_results['prescription type']=='Reversal'].index))

    assert datetime.strptime(fill_minimums["date filled"],'%Y-%m-%d') \
                >= datetime.strptime(summary_results.iloc[0]["period_start"],'%m/%d/%Y')  , \
            "All fills do not start after the invoice start date. \n" \
                "\tEarliest transaction: " + str(fill_minimums["date filled"]) + "\n" + \
                "\tSummary start date: " +str(summary_results.iloc[0]["period_start"] ) 
    
    administration_fee_total = summary_results["total administration fee"].sum(axis=0)
    calculated_administration_fee = round(transaction_results["administration fee"].sum(axis=0),2)
    assert  abs(administration_fee_total - calculated_administration_fee) < 0.01, \
        "Admin fee does not match sum of fills and reversals\n" \
            "\tTotal admin fee: " + str(administration_fee_total) + "\n" + \
            "\tTotal transactions admin fee: " + str(calculated_administration_fee)

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()
    
    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")

    print("processing: summary")    
    summary_results = redshift_src.pull("sources/Walgreens-quarterly-summary.sql")
    summary_file_name = "Hippo_Walgreens_" + "_Quarterly_Summary_" + invoice_date + ".xlsx" 

    print("processing: transactions")
    transaction_results = redshift_src.pull("sources/Walgreens-quarterly-transactions.sql")
    transaction_file_name = "Hippo_Walgreens_" + "_Quarterly_Transactions_" + invoice_date + ".csv"

    validate_results(summary_results, transaction_results)

    print("processing: admin fees")
    admin_results = redshift_src.pull("sources/Walgreens-quarterly-admin-fee-transactions.sql")
    admin_file_name = "Hippo_Walgreens_" + "_Quarterly_Admin_Fee_Transactions_" + invoice_date + ".csv"

    validate_admin_results(summary_results, admin_results)

    transactions_file_names_mapping = {transaction_file_name: transaction_results, admin_file_name: admin_results}
    
    iu.create_and_send_file("summary", summary_results, summary_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain="Walgreens", hsdk_env=hsdk_env, **kwargs)

    for filename, quarterly_file in transactions_file_names_mapping.items():
        if not quarterly_file.empty: # save as .csv in s3 bucket and text to #ivoices channel in slack
            file_bytes = io.BytesIO()
            quarterly_file.to_csv(file_bytes, index=False, encoding='utf-8')
            iu.send_file(quarterly_file, "Walgreens", filename, file_bytes,
                        slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)                     
