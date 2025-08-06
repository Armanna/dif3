#!/usr/bin/env python3

from sources import redshift
from sources import reportingdates as rd

from . import invoice_utils as iu

from datetime import datetime, timedelta
import pandas as pd
import io


def get_share(sheet_name):
    if sheet_name == 'RXInform':
        return "40%", "$1.20"
    if sheet_name == 'Rcopia':
        return "50%", "$2.50"


def create_worksheet(sheet_name, data, writer, workbook):
    worksheet = writer.sheets[sheet_name]

    background_format, date_format, money_format, number_format, \
        title_format, underline_title_format, total_format, header_format = iu.add_formats(
            workbook)

    for i in range(0, 200):
        for j in range(0, 200):
            worksheet.write(i, j, '', background_format)

    # # write titels. Can't just use from dataset as lose capitalization
    worksheet.write(3, 0, 'Quarter', header_format)
    worksheet.set_column(0, 0, 12)
    worksheet.write(3, 1, 'Month', header_format)
    worksheet.set_column(1, 1, 12)
    worksheet.write(3, 2, 'Scripts', header_format)
    worksheet.set_column(2, 2, 12)
    worksheet.write(3, 3, 'Reversals', header_format)
    worksheet.set_column(3, 3, 12)
    worksheet.write(3, 4, 'Net Revenue', header_format)
    worksheet.set_column(4, 4, 14)
    worksheet.write(3, 5, 'Net COGS', header_format)
    worksheet.set_column(5, 5, 14)
    worksheet.write(3, 6, 'Transaction Costs', header_format)
    worksheet.set_column(6, 6, 14)
    worksheet.write(3, 7, 'Net Margin', header_format)
    worksheet.set_column(7, 7, 14)
    worksheet.write(3, 8, 'Margin Less Fees', header_format)
    worksheet.set_column(8, 8, 14)

    for j in range(0, len(data.index)):
        worksheet.write(j+5, 0, data.iat[j, 0], background_format)
        worksheet.write(j+5, 1, data.iat[j, 1], date_format)
        worksheet.write(j+5, 2, data.iat[j, 2], number_format)
        worksheet.write(j+5, 3, data.iat[j, 3], number_format)
        worksheet.write(j+5, 4, data.iat[j, 4], money_format)
        worksheet.write(j+5, 5, data.iat[j, 5], money_format)
        worksheet.write(j+5, 6, data.iat[j, 6], money_format)
        worksheet.write(j+5, 7, data.iat[j, 7], money_format)
        worksheet.write(j+5, 8, data.iat[j, 8], money_format)

    percentage, per_share = get_share(sheet_name)

    worksheet.write(3, 10, percentage + ' Net Margin', header_format)
    worksheet.set_column(10, 10, 14)
    for j in range(0, len(data.index)):
        worksheet.write(j+5, 10, data.iat[j, 9], money_format)

    worksheet.write(3, 11, None, header_format)

    worksheet.write(3, 12, per_share + ' Fee / Script', header_format)
    worksheet.set_column(12, 12, 14)
    for j in range(0, len(data.index)):
        worksheet.write(j+5, 12, data.iat[j, 10], money_format)

    worksheet.write(3, 13, None, header_format)

    worksheet.write(3, 14, 'Max of Margin or Fee', header_format)
    worksheet.set_column(14, 14, 14)
    for j in range(0, len(data.index)):
        worksheet.write(j+5, 14, data.iat[j, 11], money_format)

    worksheet.write(3, 15, None, header_format)

    worksheet.write(3, 16, 'Margin on Reversaled Scripts', header_format)
    worksheet.set_column(16, 16, 14)
    for j in range(0, len(data.index)):
        worksheet.write(j+5, 16, data.iat[j, 12], money_format)

    worksheet.write(3, 17, None, header_format)

    worksheet.write(3, 18, 'DrFirst Margin', header_format)
    worksheet.set_column(18, 18, 14)
    for j in range(0, len(data.index)):
        worksheet.write(j+5, 18, data.iat[j, 13], money_format)

    worksheet.write(len(data.index) + 6, 16, 'Total', title_format)
    Last_3_month_total = data.iat[len(data.index)-1, 13] + \
        data.iat[len(data.index)-2, 13] + \
        data.iat[len(data.index)-3, 13]
    worksheet.write(len(data.index) + 6, 18,
                    Last_3_month_total, total_format)

    small_width = 2
    worksheet.set_column(11, 11, small_width)
    worksheet.set_column(13, 13, small_width)
    worksheet.set_column(15, 15, small_width)
    worksheet.set_column(17, 17, small_width)

    today = datetime.today()
    invoice_day = today.strftime("%B %d, %Y")
    worksheet.merge_range(
        'A1:W1', "DrFirst Monthly Partner Marketing Fee Calculation - As of " + invoice_day, title_format)

    worksheet.merge_range(
        'K3:S3', "DrFirst Partner Marketing Fee - " + sheet_name, underline_title_format)


def create_excel_file(RXinform_data, Rcopia_data):

    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    RXinform_data.to_excel(writer, sheet_name='RXInform',
                           startrow=5, header=False, index=False)
    Rcopia_data.to_excel(writer, sheet_name='Rcopia',
                         startrow=5, header=False, index=False)

    workbook = writer.book
    create_worksheet('RXInform', RXinform_data, writer, workbook)
    create_worksheet('Rcopia', Rcopia_data, writer, workbook)

    writer.close()
    return output


def create_invoice(RXinform_data, Rcopia_data):

    today = datetime.today()
    invoice_date = today.strftime("%m%d%y")
    quarter = rd.prior_quarter()
    invoice_file_name = "DrFirst " + \
        str(quarter) + " Margin Share v" + invoice_date + ".xlsx"

    file_bytes = create_excel_file(RXinform_data, Rcopia_data)

    return file_bytes, invoice_file_name


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()

    RXinform_results = redshift_src.pull("sources/DrFirst-invoice.sql", params={"partner": 'Dr First',
                                                                                "percent": '0.4',
                                                                                'minimum': '1.2'})
    RXinform_history = pd.read_csv("report_history/RXinform.csv")
    RXinform_results = pd.concat([RXinform_history, RXinform_results])

    Rcopia_results = redshift_src.pull("sources/DrFirst-invoice.sql", params={"partner": 'Rcopia',
                                                                              "percent": '0.5',
                                                                              'minimum': '2.25'})
    Rcopia_history = pd.read_csv("report_history/Rcopia.csv")
    Rcopia_results = pd.concat([Rcopia_history, Rcopia_results])

    if (not RXinform_results.empty) and (not Rcopia_results.empty):
        file_bytes, invoice_file_name = create_invoice(
            RXinform_results, Rcopia_results)

        if file_bytes != None:
            iu.send_file([RXinform_results, Rcopia_results], "Dr First", invoice_file_name, file_bytes,
                         slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)
        else:
            print("No data for file" + invoice_file_name)
    else:
        print("No Dr First data extracted")
