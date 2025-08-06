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

log = logger.getLogger("riteaid_invoice")

def draw_item(data,can,x,y,item_name,drug_type,basis_of_reimbursement_source,dollar=''):
    filtered_data = data.loc[(data['drug_type'] == drug_type) & \
         (data['basis_of_reimbursement_source'] == basis_of_reimbursement_source)]
    if not filtered_data.empty:
        item = filtered_data.iloc[0][item_name]
        can.drawString(x, y, dollar+str(item).strip())
        return item
    else:
        print("No item found with the given criteria")
        return None

def create_invoice(data):

    packet = io.BytesIO()
    # create a new PDF with Reportlab
    can = canvas.Canvas(packet, pagesize=letter)
    can.setFont("Helvetica", 9)

    invoice_number = data.iloc[0]['invoice_number']
    can.drawString(468, 648, str(invoice_number))
    
    invoice_date = data.iloc[0]['invoice_date']
    can.drawString(468, 634, str(invoice_date))    
    
    due_date = data.iloc[0]['due_date']
    can.drawString(468, 621, str(due_date))

    terms = data.iloc[0]['net']
    can.drawString(486, 606, str(terms))

    can.setFont("Helvetica", 10)

    period_start = data.iloc[0]['period_start']
    period_end = data.iloc[0]['period_end']
    for_period = period_start + " - " + period_end
    can.drawString(120, 437, str(for_period))
    
    draw_item(data,can,132,426,'claims count','generic','AWP')
    draw_item(data,can,152,414,'total paid claims','generic','AWP')
    draw_item(data,can,233,402,'total remunerative paid claims','generic','AWP')
    draw_item(data,can,207,390,'total ingredient cost paid','generic','AWP','$')
    admin_fee = draw_item(data,can,230,378,'total administration fee owed','generic','AWP','$')
    can.drawString(507, 450, str(admin_fee))

    can.drawString(120, 349, str(for_period))    
    draw_item(data,can,132,337,'claims count','brand','AWP')
    draw_item(data,can,152,326,'total paid claims','brand','AWP')
    draw_item(data,can,233,314,'total remunerative paid claims','brand','AWP')
    draw_item(data,can,207,302,'total ingredient cost paid','brand','AWP','$')
    admin_fee = draw_item(data,can,230,290,'total administration fee owed','brand','AWP','$')
    if admin_fee is None:
        can.drawString(552, 363, '0')
    else:
        can.drawString(502, 363, str(admin_fee))

    can.drawString(120, 262, str(for_period))    
    draw_item(data,can,132,250,'claims count','generic','NADAC')
    draw_item(data,can,152,238,'total paid claims','generic','NADAC')
    draw_item(data,can,233,226,'total remunerative paid claims','generic','NADAC')
    draw_item(data,can,207,214,'total ingredient cost paid','generic','NADAC','$')
    admin_fee = draw_item(data,can,230,202,'total administration fee owed','generic','NADAC','$')
    can.drawString(496, 273, str(admin_fee))

    can.drawString(120, 172, str(for_period))    
    draw_item(data,can,132,161,'claims count','brand','NADAC')
    draw_item(data,can,152,149,'total paid claims','brand','NADAC')
    draw_item(data,can,233,137,'total remunerative paid claims','brand','NADAC')
    draw_item(data,can,207,125,'total ingredient cost paid','brand','NADAC','$')
    admin_fee = draw_item(data,can,230,113,'total administration fee owed','brand','NADAC','$')
    if admin_fee is None:
        can.drawString(552, 182, '0')
    else:
        can.drawString(502, 182, str(admin_fee))


    can.setFont("Helvetica-Bold", 12)
    generic_data = data.loc[data['drug_type'] == 'generic']
    if not generic_data.empty:
        balance = generic_data.iloc[0]['grand total']
        if balance != 0:
            can.drawString(518, 71, '$'+ str(balance).strip())

    can.save()

    #move to the beginning of the StringIO buffer
    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    # read invoice template
    pdf_template = PdfFileReader(open("pdf/RiteAidInvoiceTemplate.pdf", "rb"))

    output = PdfFileWriter()
    # add the "watermark" (which is the new pdf) on the existing page
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)
    # finally, write "output" to a real file
    output_file = "Hippo_Rite_Aid_Invoice_" + str(invoice_number) + "_" + str(invoice_date).replace('/','') + ".pdf"
    
    outputString = io.BytesIO()        
    output.write(outputString)    

    return outputString, output_file

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, **kwargs):

    redshift_src = redshift.Redshift()
    
    invoice_results = redshift_src.pull("sources/RiteAid-invoice.sql")

    transaction_results, transaction_file_name = iu.extract_transactions(redshift_src, 'RiteAid')

    iu.validate_results(invoice_results, transaction_results)

    file_bytes, send_file_name = create_invoice(invoice_results)  

    assert file_bytes != None, "No data for invoice file creation"

    iu.send_file(invoice_results, "RiteAid", send_file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)    

    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel, 
                            slack_bot_token, invoice_bucket, chain="RiteAid", hsdk_env=hsdk_env, **kwargs)    
