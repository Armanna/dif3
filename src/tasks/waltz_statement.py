#!/usr/bin/env python3
from sources import redshift
from . import invoice_utils as iu

import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
from PyPDF2 import PdfFileWriter, PdfFileReader
from datetime import datetime
import numpy as np

CUSTOMER_LICENSEES_FEE_PER_FILL = 4
CHAIN_PHARMACY_LICENSEES_FEE_PER_FILL = 3

from hippo import logger

log = logger.getLogger("waltz_invoice")


def draw_address_details(can):
    can.setFont("Helvetica", 8)
    can.drawString(20, 635, "351 W. Hubbard St.")
    can.drawString(20, 624, "Suite #700")
    can.drawString(20, 613, "Chicago, IL 60654")


def draw_invoice_details(can, invoice_number, invoice_date, due_date, payment_period):
    can.setFont("Helvetica", 9)
    can.drawString(468, 646, str(invoice_number))
    can.drawString(468, 633, str(invoice_date))
    can.drawString(468, 609, str(due_date))
    can.drawString(482, 622, str(payment_period))


def draw_period_details(can, period_start, period_end):
    can.setFont("Helvetica", 9)
    for_period = "FOR PERIOD: " + period_start.strftime('%Y/%m/%d') + " - " + period_end.strftime('%Y/%m/%d')
    can.drawString(20, 505, str(for_period))


def calculate_licensees_data(data):
    customer_licensees_fills = ((data['reversal indicator'] == 'B1') & (data['group'] == 'WPR')).sum()
    chain_pharmacy_licensees_fills = ((data['reversal indicator'] == 'B1') & (data['group'] != 'WPR')).sum()
    customer_licensees_reversals = ((data['reversal indicator'] == 'B2') & (data['group'] == 'WPR')).sum()
    chain_pharmacy_licensees_reversals = ((data['reversal indicator'] == 'B2') & (data['group'] != 'WPR')).sum()

    customer_licensees_total = (customer_licensees_fills - customer_licensees_reversals) * CUSTOMER_LICENSEES_FEE_PER_FILL
    chain_pharmacy_licensees_total = (chain_pharmacy_licensees_fills - chain_pharmacy_licensees_reversals) * CHAIN_PHARMACY_LICENSEES_FEE_PER_FILL
    total = customer_licensees_total + chain_pharmacy_licensees_total

    return {
        'customer_licensees_fills': format(customer_licensees_fills, ','),
        'customer_licensees_reversals': format(customer_licensees_reversals, ','),
        'chain_pharmacy_licensees_fills': format(chain_pharmacy_licensees_fills, ','),
        'chain_pharmacy_licensees_reversals': format(chain_pharmacy_licensees_reversals, ','),
        'customer_licensees_total': format(customer_licensees_total, ','),
        'chain_pharmacy_licensees_total': format(chain_pharmacy_licensees_total, ','),
        'total': total
    }


def draw_table(can, licensees_totals_dict):
    table_data = [
        ['Licensees type', 'Fill count this period', 'Reversed from prior periods', 'Fee per fill', 'Total'],
        ['CUSTOMER', str(licensees_totals_dict['customer_licensees_fills']),
         str(licensees_totals_dict['customer_licensees_reversals']), "$" + str(CUSTOMER_LICENSEES_FEE_PER_FILL),
         "$" + str(licensees_totals_dict['customer_licensees_total'])],
        ['CHAIN PHARMACY', str(licensees_totals_dict['chain_pharmacy_licensees_fills']),
         str(licensees_totals_dict['chain_pharmacy_licensees_reversals']), "$" + str(CHAIN_PHARMACY_LICENSEES_FEE_PER_FILL),
         "$" + str(licensees_totals_dict['chain_pharmacy_licensees_total'])],
    ]
    table = Table(table_data, colWidths=[130, 130, 170, 70, 70])

    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, 1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ])
    table.setStyle(style)

    table.wrapOn(can, 0, 0)
    table.drawOn(can, 20, 430)


def draw_total(can, total):
    formatted_total = format(total, ',')
    can.setFont("Helvetica-Bold", 10)
    can.drawString(540, 385, "$" + str(formatted_total).strip())


def create_statement(data):
    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=letter)

    invoice_number = str(iu.get_invoice_number(64231))
    invoice_date = iu.get_prior_month_end()
    payment_period = 60
    due_date = iu.get_due_date(invoice_date, payment_period)
    period_start = iu.get_prior_month_start()
    period_end = invoice_date

    draw_address_details(can)
    draw_invoice_details(can, invoice_number, invoice_date, due_date, payment_period)
    draw_period_details(can, period_start, period_end)

    licensees_totals_dict = calculate_licensees_data(data) # calculate fill/reversal count and total per license

    draw_table(can, licensees_totals_dict) # draw the table with data for both Licensees
    draw_total(can, licensees_totals_dict['total'])  # draw the total value

    can.save()

    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    pdf_template = PdfFileReader(open("pdf/FlatFeeStatementTemplate_waltz.pdf", "rb"))

    output = PdfFileWriter()
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)

    output_file = f"Hippo_Waltz_Statement_{invoice_number}_{invoice_date.strftime('%Y%m%d')}.pdf"
    outputString = io.BytesIO()
    output.write(outputString)

    return outputString, output_file


def create_explanation_of_payment(data):
    data_copy = data.copy()
    conditions = [
        data_copy['basis of reimbursement determination'] == '04',
        data_copy['reversal indicator'] == 'B2',
        data_copy['reversal indicator'] == 'B1'
    ]
    choices = [0, -1, 1]
    data_copy['compensable count'] = np.select(conditions, choices, default=None)
    result = data_copy[['fill date', 'rx#', 'npi', 'claim authorization number', 'compensable count']]
    explanation_of_payment_file_name = f'Hippo_Waltz_Explanation_of_Payment_{datetime.today().strftime("%m%d%Y")}.xlsx'
    return result, explanation_of_payment_file_name


def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):
    redshift_src = redshift.Redshift()
    transaction_results, transaction_file_name = iu.extract_transactions(redshift_src, 'Waltz')
    file_bytes, send_file_name = create_statement(transaction_results[transaction_results[
        'basis of reimbursement determination'] != '04'])  # we don't pay for UNC fills
    assert file_bytes is not None, "No data for invoice file creation"
    iu.send_file(transaction_results, "Waltz", send_file_name, file_bytes, slack_channel, slack_bot_token,
                 invoice_bucket, hsdk_env=hsdk_env, **kwargs)
    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel,
                            slack_bot_token, invoice_bucket, chain="Waltz", hsdk_env=hsdk_env, **kwargs)
    explanation_of_payment, explanation_of_payment_file_name = create_explanation_of_payment(transaction_results)
    iu.create_and_send_file('Explanation of Payment', explanation_of_payment, explanation_of_payment_file_name,
                            slack_channel, slack_bot_token, invoice_bucket, chain="Waltz", hsdk_env=hsdk_env, **kwargs)
