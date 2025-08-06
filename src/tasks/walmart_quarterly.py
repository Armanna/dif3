#!/usr/bin/env python3

import io
from decimal import Decimal

from sources import redshift
from . import invoice_utils as iu

import pandas as pd
from datetime import datetime
from datetime import date
from hippo import logger

import numpy

log = logger.getLogger('Walmart quarterly')

AWP_DISPENSING_FEES = [Decimal('1.01'), Decimal('1.01'), Decimal('1.01'),
                       Decimal('1.01'), Decimal('1.01'), Decimal('1.01')]
NADAC_DISPENSING_FEES = [Decimal('1.01'), Decimal('1.01'), Decimal('1.01'),
                         Decimal('8.50'), Decimal('15.00'), Decimal('40.00')]
CONTRACTED_AWP_RATE = [Decimal('0.135'), Decimal('0.135'), Decimal('0.135'),
                       Decimal('0.2'), Decimal('0.2'), Decimal('0.2')]


def breakdown_quantity(df, brand_indicator, item):
    df = df[df['generic indicator (multi-source indicator)']
            == brand_indicator]
    df_1_60 = df[df["total days supply"] <= 60][item].sum()
    df_61_120 = df[(df["total days supply"] > 60) &
                   (df["total days supply"] <= 120)][item].sum()
    df_121 = df[df["total days supply"] > 120][item].sum()
    return [df_1_60, df_61_120, df_121]


def breakdown_results(df, item):
    brand = breakdown_quantity(df, 'brand', item)
    generic = breakdown_quantity(df, 'generic', item)
    return numpy.concatenate((brand, generic), axis=None)


def count_quantity(df, brand_indicator):
    df = df[df['generic indicator (multi-source indicator)']
            == brand_indicator]
    df_1_60 = len(df[df["total days supply"] <= 60].index)
    df_61_120 = len(df[(df["total days supply"] > 60) &
                       (df["total days supply"] <= 120)].index)
    df_121 = len(df[df["total days supply"] > 120].index)
    return [df_1_60, df_61_120, df_121]


def count_results(df, claim_type):
    claims = df[(df['excluded claim indicator'] == 'N') &
                (df['fill_type'] == claim_type)][["generic indicator (multi-source indicator)", "total days supply"]]
    brand = count_quantity(claims, 'brand')
    generic = count_quantity(claims, 'generic')
    return numpy.concatenate((brand, generic), axis=None)


def fill_type_breakdown(df, item, fill_type):
    result = df[(df['excluded claim indicator'] == 'N') &
                (df['fill_type'] == fill_type)][[item, "generic indicator (multi-source indicator)", "total days supply"]]
    return breakdown_results(result, item)


def get_variance(contracted, actual):
    return [a - b for a, b in zip(contracted, actual)]


def get_actual_awp_rate(df):
    contracted_awp = fill_type_breakdown(
        df, "average wholesale price (awp)", "awp")
    actual_awp = fill_type_breakdown(df, "ingredient cost paid", "awp")
    actual_awp_rate = [a / b if b != 0 else 0 for a,
                       b in zip(actual_awp, contracted_awp)]
    return [1 - x if x != 0 else 0 for x in actual_awp_rate]


def get_awp_variant_amount(actual_awp, actual_awp_rate):
    awp_rate_difference = [a - b if a != 0 else 0 for a,
                           b in zip(actual_awp_rate, CONTRACTED_AWP_RATE)]
    return [a * b for a, b in zip(actual_awp, awp_rate_difference)]


def get_variant_amount(variant):
    return numpy.concatenate((variant, [sum(variant)]), axis=None)


def get_afer(df):
    admin_fee_sum = df[df['excluded claim indicator']
                       == 'N']["administration fee"].sum()
    claims_count = len(df[df['excluded claim indicator']
                       == 'N']["administration fee"])
    return admin_fee_sum / claims_count


def previous_quarter(day):
    quarter = ((day.month)//3) + 1
    if quarter == 2:
        return "DATE RANGE: " + str(day.year) + "-01-01 - " + str(day.year) + "-03-31"
    if quarter == 3:
        return "DATE RANGE: " + str(day.year) + "-04-01 - " + str(day.year) + "-06-30"
    if quarter == 4:
        return "DATE RANGE: " + str(day.year) + "-07-01 - " + str(day.year) + "-09-30"
    elif quarter == 1:
        return "DATE RANGE: " + str(day.year-1) + "-10-01 - " + str(day.year - 1) + "-12-31"


def append_results(results, title, fields, sum):
    return_results = results.copy()
    return_results.loc[len(return_results)] = numpy.concatenate(
        ([title], fields, [sum]), axis=None)
    return return_results


def get_dispensing_fees(fill_type):
    if fill_type == 'awp':
        return AWP_DISPENSING_FEES
    else:
        return NADAC_DISPENSING_FEES


def generate_summary(transaction_results):
    contracted_nadactr = fill_type_breakdown(
        transaction_results, 'contracted_nadactr', 'nadac')
    actual_nadactr = fill_type_breakdown(
        transaction_results, 'ingredient cost paid', 'nadac')
    nadactr_variance = get_variance(contracted_nadactr, actual_nadactr)
    total_variant_amount = get_variant_amount(nadactr_variance)
    actual_awp_rate = get_actual_awp_rate(transaction_results)
    contracted_awp = fill_type_breakdown(
        transaction_results, 'average wholesale price (awp)', 'awp')
    actual_awp = fill_type_breakdown(
        transaction_results, 'ingredient cost paid', 'awp')
    awp_variant_amount = get_awp_variant_amount(contracted_awp, actual_awp_rate)
    awp_variant_amount_total = get_variant_amount(awp_variant_amount)
    total_variant_amount = [
        a + b for a, b in zip(total_variant_amount, awp_variant_amount_total)]

    date_range = previous_quarter(date.today())
    cols = [date_range, "brand 1-60", "brand 61-120",
            "brand 121+", "generic 1-60", "generic 61-120",	"generic 121+",	"total variant amt"]

    results = pd.DataFrame(columns=cols)

    EMPTY_ROW = ["", "", "", "", "", ""]
    results = append_results(
        results, "  NADAC Report",                 EMPTY_ROW, "")
    results = append_results(
        results, "    Contracted NADACTR",         contracted_nadactr, "")
    results = append_results(
        results, "    Actual NADACTR",             actual_nadactr, "")
    results = append_results(results, "    NADAC Variant Amount",
                             nadactr_variance, sum(nadactr_variance))
    results = append_results(results, "",
                             EMPTY_ROW, "")
    results = append_results(results, "  AWP Report",
                             EMPTY_ROW, "")
    results = append_results(
        results, "    Contracted AWP Rate",        CONTRACTED_AWP_RATE, "")
    results = append_results(
        results, "   Actual AWP Rate",             actual_awp_rate, "")
    results = append_results(results, "    Total AWP",
                             contracted_awp, "")
    results = append_results(
        results, "    Total Ingredient Cost Paid", actual_awp, "")
    results = append_results(results, "    AWP Variant Amount",
                             awp_variant_amount, sum(awp_variant_amount))
    results = append_results(results, "",
                             EMPTY_ROW, "")
    results = append_results(
        results, "DISPENSING FEE REPORT",          EMPTY_ROW, "")

    fill_types = ['nadac', 'awp']
    for ft in fill_types:
        claims = count_results(transaction_results, ft)
        calculated_dispensing = [a * b for a,
                                 b in zip(get_dispensing_fees(ft), claims)]
        actual_dispensing = fill_type_breakdown(
            transaction_results, "dispensing fee paid", ft)
        dispensing_fee_variant = [
            b - a for a, b in zip(actual_dispensing, calculated_dispensing)]
        dispensing_fee_variant_total = get_variant_amount(
            dispensing_fee_variant)
        total_variant_amount = [
            a + b for a, b in zip(total_variant_amount, dispensing_fee_variant_total)]

        results = append_results(results, "", EMPTY_ROW, "")
        results = append_results(
            results, "  " + ft.upper() + " Claims", EMPTY_ROW, "")
        results = append_results(results, "    Total Claims", claims, "")
        results = append_results(
            results, "    Contracted Dispensing Fee", calculated_dispensing, "")
        results = append_results(
            results, "    Actual Dispensing Fee Paid", actual_dispensing, "")
        results = append_results(results, "    Dispense Fee Variant Amount",
                                 dispensing_fee_variant, sum(dispensing_fee_variant))

    results = append_results(results, "", EMPTY_ROW, "")
    results.loc[len(results)] = numpy.concatenate(
        (["  TOTAL VARIANT AMOUNT"], total_variant_amount), axis=None)
    results = append_results(results, "", EMPTY_ROW, "")
    results.loc[len(results)] = ["  AFER", get_afer(
        transaction_results), "", "", "", "", "", ""]

    log.info(results)
    return results


def create_excel_file(data):

    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    data.to_excel(writer, sheet_name='Sheet1',
                  startrow=2, header=False, index=False)

    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.hide_gridlines()

    # write header in non-default format
    title_format = workbook.add_format({'bold': True, 'text_wrap': False,
                                        'border': 1, 'underline': 1, 'align': 'center', 'valign': 'center'})
    header_format = workbook.add_format(
        {'bold': True, 'text_wrap': False, 'border': 1})
    money_format = workbook.add_format(
        {'num_format': '$#,##0.00', 'bold': False, 'border': 1})
    percent_format = workbook.add_format(
        {'num_format': '0.00%', 'bold': False, 'border': 1})
    border_format = workbook.add_format({'text_wrap': False, 'border': 1})
    number_format = workbook.add_format(
        {'num_format': '#,###', 'text_wrap': False, 'border': 1})

    worksheet.merge_range(0, 0, 0, 7, "Discount Card Network", title_format)

    # add border
    for i in range(2, 31):  # iterate over rows
        for j in range(0, 8):  # iterate over columns
            worksheet.write(i, j, data.iat[i-2, j], border_format)

    for i in range(3, 6):  # iterate over rows
        for j in range(1, 7):  # iterate over columns
            worksheet.write(i, j, float(data.iat[i-2, j]), money_format)

    for i in range(8, 10):  # iterate over rows
        for j in range(1, 7):  # iterate over columns
            worksheet.write(i, j, float(data.iat[i-2, j]), percent_format)

    for i in range(10, 13):  # iterate over rows
        for j in range(1, 7):  # iterate over columns
            worksheet.write(i, j, float(data.iat[i-2, j]), money_format)

    for i in range(17, 19):  # iterate over rows
        for j in range(1, 7):  # iterate over columns
            worksheet.write(i, j, float(data.iat[i-2, j]), number_format)

    for i in range(18, 21):  # iterate over rows
        for j in range(1, 7):  # iterate over columns
            worksheet.write(i, j, float(data.iat[i-2, j]), money_format)

    for i in range(23, 24):  # iterate over rows
        for j in range(1, 7):  # iterate over columns
            worksheet.write(i, j, float(data.iat[i-2, j]), number_format)

    for i in range(24, 27):  # iterate over rows
        for j in range(1, 7):  # iterate over columns
            worksheet.write(i, j, float(data.iat[i-2, j]), money_format)

    for i in range(28, 29):  # iterate over rows
        for j in range(1, 8):  # iterate over columns
            worksheet.write(i, j, float(data.iat[i-2, j]), money_format)

    worksheet.write(5, 7, float(data.iat[3, 7]), money_format)
    worksheet.write(12, 7, float(data.iat[10, 7]), money_format)
    worksheet.write(20, 7, float(data.iat[18, 7]), money_format)
    worksheet.write(26, 7, float(data.iat[24, 7]), money_format)
    worksheet.write(30, 1, float(data.iat[28, 1]), money_format)

    for col_num, value in enumerate(data.columns.values):
        worksheet.write(1, col_num, value, title_format)

    # row,column
    worksheet.write(2, 0, data.iat[0, 0], header_format)
    worksheet.write(7, 0, data.iat[5, 0], header_format)
    worksheet.write(14, 0, data.iat[12, 0], header_format)
    worksheet.write(16, 0, data.iat[14, 0], header_format)
    worksheet.write(22, 0, data.iat[20, 0], header_format)
    worksheet.write(28, 0, data.iat[26, 0], header_format)

#     worksheet.write(24,1,5778.34,money_format)
#    worksheet.write(1,24,data.iat[1,24],money_format)
#     worksheet.write(7,4,0.7632,percent_format)

    # size excel columns to match the data
    for i, col in enumerate(data.columns):
        column_len = data[col].astype(str).str.len().max()
        column_len = max(column_len, len(col)) + 2
        worksheet.set_column(i, i, column_len)

    writer.close()
    return output


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()

    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")

    log.info("processing: transactions")
    transaction_results_df = redshift_src.pull(
        "sources/Walmart-quarterly-transactions.sql")
    transaction_file_name = "Hippo_Walmart_Quarterly_Transactions_" + invoice_date + ".csv"

    if not transaction_results_df.empty: # save as .csv in s3 bucket and text to #ivoices channel in slack because file is too big for excel format and for sending to slack
        file_bytes = io.BytesIO()
        transaction_results_df.to_csv(file_bytes, index=False, encoding='utf-8')
        iu.send_file(transaction_results_df, "Walmart", transaction_file_name, file_bytes,
                    slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)

    log.info("processing: summary")
    summary_results = generate_summary(transaction_results_df)
    summary_file_name = "Hippo_Walmart_Quarterly_Summary_" + invoice_date + ".xlsx"

    # split create and send. Do some post-processing
    if not summary_results.empty:
        file_bytes = create_excel_file(summary_results)

        if file_bytes != None:
            iu.send_file(summary_results, "Walmart", summary_file_name, file_bytes,
                         slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)
        else:
            log.info("no data for file" + summary_file_name)
    else:
        log.info("no data for file" + summary_file_name)
