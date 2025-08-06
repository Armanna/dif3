#!/usr/bin/env python3

import io
from decimal import Decimal, ROUND_HALF_UP
from transform import utils
from datetime import datetime

from sources import redshift
from . import invoice_utils as iu

import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from PyPDF2 import PdfFileWriter, PdfFileReader


from hippo import logger

log = logger.getLogger("rxlink_invoice")

def create_statement(data):

    packet = io.BytesIO()
    # create a new PDF with Reportlab
    can = canvas.Canvas(packet, pagesize=letter)

    can.setFont("Helvetica", 8)

    can.drawString(20, 635, "Delaware Limited Liability Company and RxLink, Inc")
    can.drawString(20, 624, "3500 S Dupont Highway, Dover")
    can.drawString(20, 613, "County of Kent, Delaware, 19901")

    can.setFont("Helvetica", 9)    
    
    invoice_number = str(iu.get_invoice_number(19901))
    can.drawString(468, 646, invoice_number)
    
    invoice_date = iu.get_prior_month_end()
    can.drawString(468, 633, str(invoice_date))    
    
    payment_period = 60
    due_date = iu.get_due_date(invoice_date, payment_period)
    can.drawString(468, 609, str(due_date))

    can.drawString(482, 622, str(payment_period))

    can.setFont("Helvetica", 10)

    period_start = iu.get_prior_month_start()
    period_end = invoice_date
    for_period = period_start.strftime('%Y/%m/%d') + " - " + period_end.strftime('%Y/%m/%d')
    can.drawString(98, 478, str(for_period))

    fills = (data['reversal indicator'] == 'B1').sum()
    can.drawString(132, 467, str(fills).strip())

    reversals = (data['reversal indicator'] == 'B2').sum()
    can.drawString(192, 456, str(reversals).strip())

    fee_per_fill = '50% of Admin Fee'
    can.drawString(82, 444, str(fee_per_fill))

    can.setFont("Helvetica", 8)
    can.drawString(20, 432, "TOTAL ADMIN FEE:")
    can.setFont("Helvetica", 10)
    can.drawString(103, 432, "$"+str((data['administration fee'].sum()).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)))

    total = (Decimal('0.5') * data['administration fee'].sum()).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    formatted_total = format(total, ',')
    can.setFont("Helvetica-Bold", 12)
    can.drawString(548, 410, "$"+str(formatted_total).strip())

    can.save()

    #move to the beginning of the StringIO buffer
    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    # read invoice template
    pdf_template = PdfFileReader(open("pdf/FlatFeeStatementTemplate.pdf", "rb"))

    output = PdfFileWriter()
    # add the "watermark" (which is the new pdf) on the existing page
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)
    # finally, write "output" to a real file
    output_file = "Hippo_RxLink_Statement_" + str(invoice_number) + "_" + str(invoice_date).replace('/','') + ".pdf"
    
    outputString = io.BytesIO()        
    output.write(outputString)    

    return outputString, output_file


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()

    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")
    period_start, period_end = utils.process_invoice_dates('month') # in order to run the script for some date in the future or past you need to set "manual_date_str" parameter i.e.: manual_date_str='2024-01-01'

    log.info('run RxLink-transactions.sql')
    transaction_results = redshift_src.pull(
        "sources/RxLink-transactions.sql", params={"period_start": period_start, "period_end": period_end}) 

    transaction_file_name = "Hippo_RxLink_Utilization_" + invoice_date + ".xlsx"
    file_bytes, send_file_name = create_statement(transaction_results[transaction_results['profitability flag'] != 'N'])  

    assert file_bytes != None, "No data for invoice file creation"

    iu.send_file(transaction_results, "RxLink", send_file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)    

    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain="RxLink", hsdk_env=hsdk_env, **kwargs)    
