#!/usr/bin/env python3

from os import environ, remove
import os
import sys

from decimal import Decimal, ROUND_HALF_UP
from sources import redshift
from . import invoice_utils as iu

import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from PyPDF2 import PdfFileWriter, PdfFileReader

from hippo import logger

log = logger.getLogger("caprx_invoice")


def create_statement(data):
    packet = io.BytesIO()
    # create a new PDF with Reportlab
    can = canvas.Canvas(packet, pagesize=letter)

    can.setFont("Helvetica", 8)

    can.drawString(20, 635, "Magellan Rx Management LLC")
    can.drawString(20, 624, "2900 Ames Crossing Rd.")
    can.drawString(20, 613, "Eagan, MN 55121")

    can.setFont("Helvetica", 9)

    invoice_number = str(iu.get_invoice_number(30104))
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
    can.setFont("Helvetica", 8)
    can.drawString(20, 478, "FOR PERIOD:")
    can.drawString(78, 478, str(for_period))

    net_claims = data['claim count indicator'].sum()
    can.drawString(20, 467, "NET NUMBER OF CLAIMS:")
    can.drawString(130, 467, str(net_claims))


    net_compensable_claims = data[(data['compensable count indicator'] == 1) & (data['claim count indicator'] == 1)]['compensable count indicator'].sum()
    can.drawString(20, 456, "COMPENSABLE CLAIMS COUNT THIS PERIOD:")
    can.drawString(204, 456, str(net_compensable_claims))

    compensable_reversals = data[(data['compensable count indicator'] == 1) & (data['claim count indicator'] == -1)]['compensable count indicator'].sum()
    can.drawString(20, 444, "COMPENSABLE REVERSALS COUNT FROM PRIOR PERIODS:")
    can.drawString(260, 444, str(compensable_reversals))

    can.drawString(20, 432, "FEE PER FILL:")
    fee_per_fill = '$1'
    can.drawString(80, 432, str(fee_per_fill))

    net_fills = net_compensable_claims - compensable_reversals

    can.drawString(20, 421, "TOTAL FEE:")
    can.setFont("Helvetica", 8)
    can.drawString(70, 421,
                   "$" + format(net_fills, ',') + ".00")

    formatted_total = format(net_fills, ',')
    can.setFont("Helvetica-Bold", 12)
    can.drawString(525, 388, "$" + str(formatted_total).strip()  + ".00")

    can.save()

    # move to the beginning of the StringIO buffer
    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    # read invoice template
    pdf_template = PdfFileReader(open("pdf/FlatFeeStatementTemplate_caprx.pdf", "rb"))

    output = PdfFileWriter()
    # add the "watermark" (which is the new pdf) on the existing page
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)
    # finally, write "output" to a real file
    output_file = "Hippo_Caprx_Statement_" + str(invoice_number) + "_" + str(invoice_date).replace('/', '') + ".pdf"

    outputString = io.BytesIO()
    output.write(outputString)

    return outputString, output_file


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):
    redshift_src = redshift.Redshift()

    transaction_results, transaction_file_name = iu.extract_transactions(redshift_src, 'Caprx')

    file_bytes, send_file_name = create_statement(transaction_results)

    assert file_bytes != None, "No data for invoice file creation"

    iu.send_file(transaction_results, "Caprx", send_file_name, file_bytes, slack_channel, slack_bot_token,
                 invoice_bucket, hsdk_env=hsdk_env, **kwargs)

    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel,
                            slack_bot_token, invoice_bucket, chain="Caprx", hsdk_env=hsdk_env, **kwargs)
