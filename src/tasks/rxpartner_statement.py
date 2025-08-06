from sources import redshift
from . import invoice_utils as iu

import io
import pandas as pd
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
from PyPDF2 import PdfFileWriter, PdfFileReader
from datetime import timedelta
from hippo import logger

log = logger.getLogger("rxpartner_invoice")

PAID_FEE_PER_FILL = 4
PARTIALLY_PAID_FEE_PER_FILL = '90%'

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):
    redshift_src = redshift.Redshift()
    transaction_results, transaction_file_name = iu.extract_transactions(redshift_src, 'RxPartner')
    file_bytes, send_file_name = create_statement(transaction_results)
    assert file_bytes is not None, "No data for ststement file creation"
    iu.send_file(transaction_results, "RxPartner", send_file_name, file_bytes, slack_channel, slack_bot_token,
                 invoice_bucket, hsdk_env=hsdk_env, **kwargs)
    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel,
                            slack_bot_token, invoice_bucket, chain="RxPartner", hsdk_env=hsdk_env, **kwargs)


def calculate_fills(data, period_start):
    period_start = pd.to_datetime(period_start)
    data['fill date'] = pd.to_datetime(data['fill date'])
    paid = data[data['partial_paid_flag'] == False]
    partially_paid = data[data['partial_paid_flag'] == True]
    paid_fills = paid[paid['reversal indicator'] == 'B1']
    partially_paid_fills = partially_paid[partially_paid['reversal indicator'] == 'B1']
    paid_reversals = paid[paid['reversal indicator'] == 'B2']
    partially_paid_reversals = partially_paid[partially_paid['reversal indicator'] == 'B2']
    paid_fill_count_this_period = paid_fills[(paid_fills["fill date"] >= period_start)].shape[0]
    partially_paid_fill_count_this_period = partially_paid_fills[(partially_paid_fills["fill date"] >= period_start)].shape[0]
    paid_reversed_from_previous_periods = paid_reversals[paid_reversals["fill date"] < period_start].shape[0]
    partially_paid_reversed_from_previous_periods = partially_paid_reversals[partially_paid_reversals["fill date"] < period_start].shape[0]
    paid_net_claims = paid_fill_count_this_period - paid_reversed_from_previous_periods
    partially_paid_net_claims = partially_paid_fill_count_this_period - partially_paid_reversed_from_previous_periods
    paid_total = paid_net_claims * PAID_FEE_PER_FILL
    partially_paid_total = partially_paid_fills['rxpartner_margin'].sum()
    total = paid_total + partially_paid_total
    return {
        "paid": {
            "fill_count_this_period": format(paid_fills.shape[0], ','),
            "reversed_from_prior_periods": format(paid_reversed_from_previous_periods, ','),
            "net_claims": format(paid_net_claims, ','),
            "fee_per_fill": format(PAID_FEE_PER_FILL, ','),
            "total": format(round(paid_total, 2), ',')
    },
        "partially_paid": {
            "fill_count_this_period": partially_paid_fills.shape[0],
            "reversed_from_prior_periods": partially_paid_reversed_from_previous_periods,
            "net_claims": partially_paid_net_claims,
            "fee_per_fill": PARTIALLY_PAID_FEE_PER_FILL,
            "total": round(partially_paid_total, 2)
    },
        "total": round(total, 2)
}


def create_statement(data):
    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=letter)

    invoice_number = str(iu.get_invoice_number(61324))
    period_end = iu.get_prior_month_end()
    payment_period = 45
    period_start = iu.get_prior_month_start()
    invoice_date = period_end + timedelta(days=1)
    due_date = iu.get_due_date(invoice_date, payment_period)

    draw_address_details(can)
    draw_invoice_details(can, invoice_number, invoice_date, due_date, payment_period)
    draw_period_details(can, period_start, period_end)

    results = calculate_fills(data, period_start)


    draw_table(can, results) # draw the table with data for both Licensees
    draw_total(can, results['total'])  # draw the total value

    can.save()

    packet.seek(0)
    new_pdf = PdfFileReader(packet, strict=False)
    pdf_template = PdfFileReader(open("pdf/FlatFeeStatementTemplate_rxpartner.pdf", "rb"))

    output = PdfFileWriter()
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)

    output_file = f"Hippo_RxPartner_Revenue_Report_{invoice_number}_{invoice_date.strftime('%Y%m%d')}.pdf"
    outputString = io.BytesIO()
    output.write(outputString)

    return outputString, output_file


def draw_address_details(can):
    can.setFont("Helvetica", 8)
    can.drawString(20, 635, "ALFA RX LLC d/b/a RxPartner")
    can.drawString(20, 624, "3680 Wilshire Blvd, Ste 4 ")
    can.drawString(20, 613, "Los Angeles, CA 90010")


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

def draw_total(can, total):
    formatted_total = format(total, ',')
    can.setFont("Helvetica-Bold", 10)
    can.drawString(540, 385, "$" + str(formatted_total).strip())


def draw_table(can, data):
    table_data = [
        ['', 'Fill count this period', 'Reversed from prior periods', 'Net claims', 'Fee per fill', 'Total'],
        ['Paid claims', str(data['paid']['fill_count_this_period']),
         str(data['paid']['reversed_from_prior_periods']), str(data['paid']['net_claims']),
         "$" + str(data['paid']['fee_per_fill']), "$" + str(data['paid']['total'])],
        ['Partially paid claims', str(data['partially_paid']['fill_count_this_period']),
         str(data['partially_paid']['reversed_from_prior_periods']), str(data['partially_paid']['net_claims']),
         data['partially_paid']['fee_per_fill'], "$" + str(data['partially_paid']['total'])],
    ]
    table = Table(table_data, colWidths=[105, 110, 150, 70, 70])

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
