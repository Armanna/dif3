#!/usr/bin/env python3

from os import environ, remove
import os
import sys

from sources import redshift
from . import invoice_utils as iu

import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from PyPDF2 import PdfFileWriter, PdfFileReader


from hippo import logger

log = logger.getLogger("famulus_invoice")

def create_statement(data):

    packet = io.BytesIO()
    # create a new PDF with Reportlab
    can = canvas.Canvas(packet, pagesize=letter)

    can.setFont("Helvetica", 8)

    can.drawString(20, 635, "Famulus Health LLC")
    can.drawString(20, 624, "20 Towne Dr")
    can.drawString(20, 613, "Bluffton, SC 29910")

    can.setFont("Helvetica", 9)    
    
    invoice_number = str(iu.get_invoice_number(31030))
    can.drawString(468, 646, invoice_number)
    
    invoice_date = iu.get_prior_month_end()
    can.drawString(468, 633, str(invoice_date))    
    
    payment_period = 45
    due_date = iu.get_due_date(invoice_date,payment_period)
    can.drawString(468, 609, str(due_date))

    can.drawString(482, 622, str(payment_period))

    can.setFont("Helvetica", 10)

    period_start = iu.get_prior_month_start()
    period_end = invoice_date
    for_period = period_start.strftime('%Y/%m/%d') + " - " + period_end.strftime('%Y/%m/%d')
    can.drawString(87, 497, str(for_period))
    
    total_fills = len(data[(data['reversal indicator'] == 'B1')])
    can.drawString(132, 486, str(total_fills).strip())

    total_reversals = len(data[(data['reversal indicator'] == 'B2')])
    can.drawString(192, 474, str(total_reversals).strip())

    compensable_fills = len(data[(data['reversal indicator'] == 'B1') & (data['excluded claim indicator'] == 'N')])
    can.drawString(141, 463, str(compensable_fills).strip())

    compensable_reversals = len(data[(data['reversal indicator'] == 'B2') & (data['excluded claim indicator'] == 'N')])
    can.drawString(258, 451, str(compensable_reversals).strip())
    
    fee_per_fill = 3
    can.drawString(148, 440, "$"+str(fee_per_fill).strip())
    
    total = 3 * (compensable_fills - compensable_reversals)
    formatted_total = format(total, ',')
    can.setFont("Helvetica-Bold", 12)
    can.drawString(548, 410, "$"+str(formatted_total).strip())

    can.save()

    #move to the beginning of the StringIO buffer
    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    # read invoice template
    pdf_template = PdfFileReader(open("pdf/FamulusFlatFeeStatementTemplate.pdf", "rb"))

    output = PdfFileWriter()
    # add the "watermark" (which is the new pdf) on the existing page
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)
    # finally, write "output" to a real file
    output_file = "Hippo_Famulus_Statement_" + str(invoice_number) + "_" + str(invoice_date).replace('/','') + ".pdf"
    
    outputString = io.BytesIO()        
    output.write(outputString)    

    return outputString, output_file


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()    

    transaction_results, transaction_file_name = iu.extract_transactions(redshift_src, 'Famulus')

    file_bytes, send_file_name = create_statement(transaction_results)  

    assert file_bytes != None, "No data for invoice file creation"

    iu.send_file(transaction_results, "Famulus", send_file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)    

    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain="Famulus", hsdk_env=hsdk_env, **kwargs)    
