import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import decimal
import io

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


from reportlab.pdfgen import canvas
from PyPDF2 import PdfFileWriter, PdfFileReader
from reportlab.lib.pagesizes import letter
from store.s3 import S3
from requests_toolbelt.multipart.encoder import MultipartEncoder

from hippo import logger

log = logger.getLogger("invoice_utils")


def send_file(data, recipient, file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket, **kwargs):
    client = WebClient(token=slack_bot_token)

    log.info("uploading file " + file_name)

    if 'hsdk_env' in kwargs and kwargs['hsdk_env'] == 'production' and kwargs['test_run_flag'] != 'True':
        slack_channel_number = slack_channel # send message to #invoices channel
        test_prefix = ''
    elif 'test_run_flag' in kwargs and kwargs['test_run_flag'] == 'True':
        slack_channel_number = 'C057UP179AB' # send message to #temp_invoices channel
        test_prefix = 'temp/' # if it's test run - add /temp prefix
    else:
        slack_channel_number = 'C057UP179AB' # send message to #temp_invoices channel
        test_prefix = ''

    S3().upload_fileobj(file_bytes.getvalue(), str(
        invoice_bucket), test_prefix + recipient.lower() + "/" + file_name)

    if isinstance(data, list):
        for dat in data:
            S3().upload_fileobj(dat.to_csv(index=False), str(
                invoice_bucket), test_prefix + recipient.lower() + "/" + file_name + ".csv")
    else:
        S3().upload_fileobj(data.to_csv(index=False), str(
            invoice_bucket), test_prefix + recipient.lower() + "/" + file_name + ".csv")

    try:
        result = client.chat_postMessage(
            channel=slack_channel_number,
            text = (
                f"{recipient} CSV file uploaded to S3 bucket: Download command:\n"
                "```\n"
                f"aws s3 cp s3://{invoice_bucket}/{test_prefix + recipient.lower()}/{file_name} ~/Downloads/\n"
                "```"
            )
        )

    # Log the result
        log.info(result)

    except SlackApiError as e:
        log.info(f"Error sending message to Slack: {e.response['error']}")

    f = open(file_name, "wb")
    f.write(file_bytes.getvalue())
    f.close()

def send_file_python(data, chain, file_name, file_bytes, temp_slack_channel, slack_bot_token, invoice_bucket):
    client = WebClient(token=slack_bot_token) 

    log.info("uploading file " + file_name)

    S3().upload_fileobj(file_bytes.getvalue(), str(
        invoice_bucket), "temp/" + chain.lower() + "/" + file_name)

    if isinstance(data, list):
        for dat in data:
            S3().upload_fileobj(dat.to_csv(index=False), str(
                invoice_bucket), "temp/" + chain.lower() + "/" + file_name + ".csv")
    else:
        S3().upload_fileobj(data.to_csv(index=False), str(
            invoice_bucket), "temp/" + chain.lower() + "/" + file_name + ".csv")

    # write the file to file system if testing
    # f = open(file_name, "wb")
    # f.write(file_bytes.getvalue())
    # f.close()

    try:
        result = client.files_upload(
            channels=temp_slack_channel,
            file=file_bytes.getvalue(),
            filename=file_name,
            title=file_name,
        )

    # Log the result
        log.info(result)

    except SlackApiError as e:
        log.info("Error uploading file: {} to slack".format(e))


def create_excel_file(data):

    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    data.to_excel(writer, sheet_name='Sheet1',
                  startrow=1, header=False, index=False)

    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # write header in non-default format
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': False,
        'border': 0})

    for col_num, value in enumerate(data.columns.values):
        worksheet.write(0, col_num, value, header_format)

    # size excel columns to match the data
    for i, col in enumerate(data.columns):
        column_len = data[col].astype(str).str.len().max()
        column_len = max(column_len, len(col)) + 2
        worksheet.set_column(i, i, column_len)

    writer.close()
    return output


def add_formats(workbook):
    background_format = workbook.add_format(
        {'bg_color': 'white'})
    date_format = workbook.add_format(
        {'num_format': 'mm/dd/yy', 'align': 'center', 'bg_color': 'white', 'font_size': 12, })
    money_format = workbook.add_format(
        {'num_format': '$#,##0.00', 'bold': False, 'border': 0, 'bg_color': 'white', 'font_size': 12, })
    number_format = workbook.add_format(
        {'num_format': '#,###', 'text_wrap': False, 'border': 0, 'bg_color': 'white', 'font_size': 12, })
    title_format = workbook.add_format(
        {'bold': True, 'text_wrap': False, 'bg_color': 'white', 'border': 0, 'font_size': 12, 'align': 'center', })
    underline_title_format = workbook.add_format({
        'bold': True, 'text_wrap': False, 'bg_color': 'white', 'border': 0, 'bottom': 1, 'font_size': 12, 'align': 'center', })
    total_format = workbook.add_format({
        'num_format': '$#,##0.00', 'bold': True, 'text_wrap': False, 'bg_color': 'white', 'border': 0, 'bottom': 6, 'font_size': 12,
        'align': 'center', 'top': 1, })
    header_format = workbook.add_format({
        'bold': True, 'text_wrap': True, 'bg_color': 'white', 'valign': 'vcenter', 'border': 0, 'bottom': 1, 'font_size': 12,
        'bottom_color': 'black', 'align': 'center', })

    return background_format, date_format, money_format, number_format, \
        title_format, underline_title_format, total_format, header_format


def create_invoice(data, chain, vendor_number, addr_line_1="", addr_line_2="", addr_line_3="", addr_line_4="", addr_line_5=""):

    packet = io.BytesIO()
    # create a new PDF with Reportlab
    can = canvas.Canvas(packet, pagesize=letter)
    can.setFont("Helvetica", 8)

    can.drawString(20, 635, addr_line_1)
    can.drawString(20, 624, addr_line_2)
    can.drawString(20, 613, addr_line_3)
    can.drawString(20, 602, addr_line_4)
    can.drawString(20, 591, addr_line_5)

    invoice_number = data.iloc[0]['invoice_number']
    can.drawString(492, 646, str(invoice_number))

    can.drawString(20, 560, vendor_number)

    invoice_date = data.iloc[0]['invoice_date']
    can.drawString(492, 634, str(invoice_date))

    terms = data.iloc[0]['net']
    can.drawString(507, 622, str(terms))

    due_date = data.iloc[0]['due_date']
    can.drawString(492, 611, str(due_date))

    can.setFont("Helvetica", 9)

    period_start = data.iloc[0]['period_start']
    period_end = data.iloc[0]['period_end']
    for_period = period_start + " - " + period_end
    can.drawString(84, 479, str(for_period))

    claims_count = data.loc[data['drug_type']
                            == 'generic'].iloc[0]['claims count']
    can.drawString(90, 468, str(claims_count))

    paid_claims = data.loc[data['drug_type'] ==
                           'generic'].iloc[0]['total paid claims']
    can.drawString(106, 458, str(paid_claims))

    remunerative_claims = data.loc[data['drug_type'] ==
                                   'generic'].iloc[0]['total remunerative paid claims']
    can.drawString(171, 447, str(remunerative_claims))

    ingredient_cost = data.loc[data['drug_type'] ==
                               'generic'].iloc[0]['total ingredient cost paid']
    can.drawString(155, 436, '$' + str(ingredient_cost).strip())

    admin_fee = data.loc[data['drug_type'] ==
                         'generic'].iloc[0]['total administration fee owed']
    can.drawString(174, 426, '$' + str(admin_fee).strip())

    can.setFont("Helvetica", 10)
    can.drawString(522, 484, str(admin_fee))

    can.setFont("Helvetica", 9)
    can.drawString(84, 397, str(for_period))

    if (data['drug_type'] == 'brand').any():
        claims_count = data.loc[data['drug_type']
                                == 'brand'].iloc[0]['claims count']
        can.drawString(90, 386, str(claims_count))
    
        paid_claims = data.loc[data['drug_type'] ==
                            'brand'].iloc[0]['total paid claims']
        can.drawString(106, 376, str(paid_claims))

        remunerative_claims = data.loc[data['drug_type'] ==
                                    'brand'].iloc[0]['total remunerative paid claims']
        can.drawString(171, 365, str(remunerative_claims))

        ingredient_cost = data.loc[data['drug_type'] ==
                                'brand'].iloc[0]['total ingredient cost paid']
        can.drawString(155, 354, '$' + str(ingredient_cost).strip())

        admin_fee = data.loc[data['drug_type'] ==
                            'brand'].iloc[0]['total administration fee owed']
        can.drawString(174, 344, '$' + str(admin_fee).strip())

        can.setFont("Helvetica", 10)
        can.drawString(522, 405, str(admin_fee))

        can.setFont("Helvetica-Bold", 12)
    
    balance = data.loc[data['drug_type'] == 'generic'].iloc[0]['grand total']
    can.drawString(524, 306, '$' + str(balance).strip())

    can.save()

    # move to the beginning of the StringIO buffer
    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    # read invoice template
    pdf_template = PdfFileReader(open("pdf/invoiceTemplate.pdf", "rb"))

    output = PdfFileWriter()
    # add the "watermark" (which is the new pdf) on the existing page
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)
    # finally, write "output" to a real file
    output_file = "Hippo_" + chain + "_Invoice_" + \
        str(invoice_number) + "_" + str(invoice_date).replace('/', '') + ".pdf"

    outputString = io.BytesIO()
    output.write(outputString)

    return outputString, output_file


def extract_summary(redshift_src, chain):

    log.info("processing: summary")
    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")

    sql_file = "sources/" + chain + "-quarterly-summary.sql"
    results = redshift_src.pull(sql_file)
    summary_file = "Hippo_" + chain + "_Quarterly_Summary_" + invoice_date + ".xlsx"

    return results, summary_file


def extract_transactions(redshift_src, chain):

    log.info("processing: transactions")
    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")

    sql_file = "sources/" + chain + "-transactions.sql"
    results = redshift_src.pull(sql_file)
    utilization_file = "Hippo_" + chain + "_Utilization_" + invoice_date + ".xlsx"

    return results, utilization_file


def get_invoice_number(seed, precise=False):
    start_date = datetime.strptime('20210401', '%Y%m%d')    
    current_date = datetime.now()

    # Calculate the difference
    if precise:
        difference = (current_date - start_date).seconds
        time.sleep(1)  # guarantee that it will be unique
    else:
        difference = (current_date - start_date).days // 7

    # Calculate the invoice number
    return seed + difference

def get_prior_month_end():
    current_date = datetime.now()

    # Truncate to the start of the month
    start_of_current_month = current_date.replace(day=1)

    # Subtract one day to get the last day of the previous month
    period_end = start_of_current_month - timedelta(days=1)

    return period_end.date()

def get_prior_month_start():
    current_date = datetime.now()
    
    # Truncate to the start of the month
    start_of_current_month = current_date.replace(day=1)

    # Subtract one month
    period_start = start_of_current_month - relativedelta(months=1)

    return period_start.date()

def get_due_date(invoice_date, payment_period):
    return invoice_date + timedelta(days=payment_period)

def validate_results(invoice_results, transaction_results):
    for dataframe in [invoice_results, transaction_results]:
        dataframe.columns = dataframe.columns.str.lower()

    invoice_totals = invoice_results.sum(axis=0)

    # filter out unc transactions which are in transaction files but not in invoice
    transaction_totals = transaction_results[transaction_results['basis of reimbursement determination'] != '04'].sum(
        axis=0)
    fill_minimums = transaction_results[transaction_results['reversal indicator'] == 'B1'].min(
        axis=0)
    reversal_minimums = transaction_results[transaction_results['reversal indicator'] == 'B2'].min(
        axis=0)
    reversal_maximums = transaction_results[transaction_results['reversal indicator'] == 'B2'].max(
        axis=0)
    transaction_maximums = transaction_results.max(axis=0)

    assert invoice_totals["claims count"] == \
        len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index) \
        - len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index), \
        "Total claims in invoice do not match fills minus reversals.\n" + \
        "\tTotal claims count: " + str(invoice_totals["claims count"]) + "\n" + \
        "\tTotal fills: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index)) + "\n" + \
        "\tTotal reversals: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index)) + "\n" +  \
        "\tCalculated fill count: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index)
                                          - len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index))

    assert datetime.strptime(fill_minimums["fill date"], '%Y-%m-%d') \
        >= datetime.strptime(invoice_results.iloc[0]["period_start"], '%m/%d/%Y'), \
        "All fills do not start after the invoice start date. \n" \
        "\tEarliest transaction: " + str(fill_minimums["fill date"]) + "\n" + \
        "\tInvoice start date: " + str(invoice_results.iloc[0]["period_start"])

    # not all invoices have reversals yet
    # assert datetime.strptime(reversal_minimums["reversal date"],'%Y-%m-%d') \
    #              >= datetime.strptime(invoice_results.iloc[0]["period_start"],'%m/%d/%Y')  , \
    #     "All transaction were not done after the invoice start date\n" \
    #         "\tEarliest transaction date: " + str(reversal_minimums["reversal date"]) + "\n" + \
    #         "\tInvoice start date: " +str(invoice_results.iloc[0]["period_start"] )

    # assert datetime.strptime(reversal_maximums["fill date"],'%Y-%m-%d') \
    #              < datetime.strptime(invoice_results.iloc[0]["period_start"],'%m/%d/%Y'), \
    #     "All reversals were not for fills made before the invoice start date\n" \
    #         "\tlatest transaction date: " + str(reversal_maximums["fill date"]) + "\n" + \
    #         "\tInvoice start date: " +str(invoice_results.iloc[0]["period_start"] )

    invoice_ingredient_cost = pd.to_numeric(
        invoice_results["total ingredient cost paid"].str.replace(' ', '').str.replace(',', ''))
    ingredient_total = decimal.Decimal(invoice_ingredient_cost.sum(axis=0))
    calculated_ingredient_cost = transaction_totals["ingredient cost paid"]
    assert abs(ingredient_total - calculated_ingredient_cost) < 0.01, \
        "Ingredient cost does not match transactions ingredient costs\n" \
        "\tTotal ingredient cost: " + str(ingredient_total) + "\n" + \
        "\tTotal transaction ingredient cost: " + \
        str(calculated_ingredient_cost)

    administration_fee = pd.to_numeric(
        invoice_results["total administration fee owed"].str.replace(' ', '').str.replace(',', ''))
    administration_fee_total = decimal.Decimal(administration_fee.sum(axis=0))#.quantize(decimal.Decimal('0.01'))
    calculated_administration_fee = round(transaction_totals["administration fee"], 2)
    assert abs(administration_fee_total - calculated_administration_fee) < 0.01, \
        "Admin fee does not match sum of fills and reversals\n" \
        "\tTotal admin fee: " + str(administration_fee_total) + "\n" + \
        "\tTotal transactions admin fee: " + \
        str(calculated_administration_fee)


def create_and_send_file(file_type, results, file_name, slack_channel,
                         slack_bot_token, invoice_bucket, chain, **kwargs):

    if not results.empty:
        if file_type == 'invoice':
            file_bytes, send_file_name = create_invoice(results, chain, kwargs["vendor_number"],
                                                        kwargs["addr_line_1"], kwargs["addr_line_2"], kwargs["addr_line_3"], kwargs["addr_line_4"], kwargs["addr_line_5"])
        else:
            file_bytes = create_excel_file(results)
            send_file_name = file_name

        if file_bytes != None:
            send_file(results, chain, send_file_name, file_bytes, slack_channel,
                      slack_bot_token, invoice_bucket, **kwargs)
        else:
            log.info("no data for file" + file_name)
    else:
        log.info("no data for file" + file_name)

def create_and_send_file_python(file_type, results, file_name, temp_slack_channel,
                         slack_bot_token, invoice_bucket, chain, **kwargs):

    if not results.empty:
        if file_type == 'invoice':
            file_bytes, send_file_name = create_invoice(results, chain, kwargs["vendor_number"],
                                                        kwargs["addr_line_1"], kwargs["addr_line_2"], kwargs["addr_line_3"], kwargs["addr_line_4"], kwargs["addr_line_5"])
        else:
            file_bytes = create_excel_file(results)
            send_file_name = file_name

        if file_bytes != None:
            send_file_python(results, chain, send_file_name, file_bytes, temp_slack_channel,
                      slack_bot_token, invoice_bucket)
        else:
            log.info("no data for file" + file_name)
    else:
        log.info("no data for file" + file_name)

def send_text_message_to_slack(slack_channel, slack_bot_token, text_msg):
    # send text message to the specific slack channel
    client = WebClient(token=slack_bot_token)

    try:
        result = client.chat_postMessage(
            channel=slack_channel,
            text=text_msg
        )
        # Log the result
        log.info(result)

    except SlackApiError as e:
        log.info(f"Error sending message to Slack: {e.response['error']}")
