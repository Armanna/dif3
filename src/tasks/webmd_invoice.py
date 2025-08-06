#!/usr/bin/env python3

from tasks import invoice_utils as utils
from sources import redshift
from . import invoice_utils as iu

from datetime import datetime, timedelta
import pandas as pd
import io


def create_excel_file(data, deductible_results):

    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    data.to_excel(writer, sheet_name='Sheet1',
                  startrow=5, header=False, index=False)

    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    background_format = workbook.add_format({'bg_color': 'white'})
    date_format = workbook.add_format(
        {'num_format': 'mmm-yy', 'align': 'center'})
    money_format = workbook.add_format(
        {'num_format': '$#,##0.00', 'bold': False, 'border': 0, 'bg_color': 'white'})
    title_money_format = workbook.add_format(
        {'num_format': '$#,##0.00', 'bold': True, 'border': 0, 'bg_color': 'white'})
    number_format = workbook.add_format(
        {'num_format': '#,###', 'text_wrap': False, 'border': 0, 'bg_color': 'white'})

    for i in range(0, 200):
        for j in range(0, 200):
            worksheet.write(i, j, '', background_format)

    # write header in non-default format
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'bg_color': 'white',
        'border': 0,
        'bottom': 1,
        'font_size': 12,
        'bottom_color': 'black',
        'align': 'center',
    })

    # for col_num, value in enumerate(data.columns.values):
    #     worksheet.write(4, col_num, value, header_format)

    # write invoice titles. Can't just use from dataset as lose capitalization
    worksheet.write(4, 0, 'Month', header_format)
    worksheet.write(4, 1, 'Fills', header_format)
    worksheet.write(4, 2, 'Reversals', header_format)
    worksheet.write(4, 3, 'Fills Less Reversals', header_format)    
    worksheet.write(4, 4, 'Penny Fills', header_format)
    worksheet.write(4, 5, 'Penny Reversals', header_format)
    worksheet.write(4, 6, 'Total Compensable Claims', header_format)
    worksheet.write(4, 7, 'WebMd Margin Due', header_format)

    # write invoice data with correct formatting
    worksheet.write(5, 0, data['month'].iloc[0], date_format)
    worksheet.write(5, 1, data['fills'].iloc[0], number_format)
    worksheet.write(5, 2, data['reversals'].iloc[0], number_format)
    worksheet.write(5, 3, data['net_fills'].iloc[0], number_format)
    worksheet.write(5, 4, data['penny_fills'].iloc[0], number_format)
    worksheet.write(5, 5, data['penny_reversals'].iloc[0], number_format)
    worksheet.write(5, 6, data['net_fills'].iloc[0] - data['net_penny_fills'].iloc[0], number_format)
    worksheet.write(5, 7, data['webmd_margin'].iloc[0], money_format)

    if not deductible_results.empty:
        deductible_value = deductible_results['webmd_deduction'].iloc[0]
        # write deduction titles.
        worksheet.write(7, 0, 'Month', header_format)
        worksheet.write(7, 1, 'Balance Users', header_format)
        worksheet.write(7, 2, 'Fill count', header_format)
        worksheet.write(7, 3, 'Deduction', header_format)    
        worksheet.write(7, 4, 'Remaining Balance', header_format)

        # write deduction data with correct formatting
        worksheet.write(8, 0, deductible_results['month'].iloc[0], date_format)
        worksheet.write(8, 1, deductible_results['balance_users'].iloc[0], number_format)
        worksheet.write(8, 2, deductible_results['total_fills'].iloc[0], number_format)
        worksheet.write(8, 3, deductible_results['webmd_deduction'].iloc[0], money_format)
        worksheet.write(8, 4, deductible_results['webmd_remaining_balance'].iloc[0], money_format)
    else:
        deductible_value = 0 # in case there is no deductible in the specific month


    # size excel columns to match the data
    for i, col in enumerate(data.columns):
        column_len = data[col].astype(str).str.len().max() + 4
        worksheet.set_column(i, i, column_len)

    worksheet.set_column(0, 10, 30)

    # write header in non-default format
    title_format = workbook.add_format({
        'bold': True,
        'text_wrap': False,
        'bg_color': 'white',
        'border': 0,
        'font_size': 12,
    })

    worksheet.write(0, 0, 'Hippo Network LLC', title_format)

    last_month = datetime.utcnow().replace(day=1) - \
        timedelta(days=1)
    invoice_month = last_month.strftime("%b %Y")
    worksheet.write(1, 0, 'WebMd Monthly Margin Due for ' +
                    invoice_month, title_format)

    today = datetime.today()
    invoice_day = today.strftime("%B %d, %Y")
    worksheet.write(2, 0, 'Report as of ' +
                    invoice_day, title_format)

    worksheet.write(10, 0, 'Total Due:', title_format)
    worksheet.set_column(0, 0, 20)
    worksheet.write(10, 1, data.iat[0, 14] - deductible_value, title_money_format)

    writer.close()
    return output


def create_invoice(data, deductible_results):

    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")
    invoice_file_name = "Hippo_WebMd_Statement_" + invoice_date + ".xlsx"

    file_bytes = create_excel_file(data, deductible_results)

    return file_bytes, invoice_file_name


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()

    invoice_results = redshift_src.pull("sources/WebMd-invoice.sql")
    deductible_results = redshift_src.pull("sources/WebMD-deduction.sql")
    invoice_month = invoice_results['month'].iloc[0]
    deductible_results = deductible_results[deductible_results['month'] == invoice_month]

    if deductible_results.empty:
        text_msg = f"No data for deductible found in swamp.balance_hasher_v4 table. Reporting month is {invoice_month}. Worth to investigate before sending invoice."
        utils.send_text_message_to_slack(slack_channel, slack_bot_token, text_msg)

    if not invoice_results.empty:
        file_bytes, invoice_file_name = create_invoice(invoice_results, deductible_results)

        if file_bytes != None:
            iu.send_file(invoice_results, "WebMd", invoice_file_name, file_bytes,
                         slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)
        else:
            print("No data for file" + invoice_file_name)
    else:
        print("No WebMd data extracted")
