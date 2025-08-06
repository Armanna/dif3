import io
from decimal import Decimal
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import Color, grey
from PyPDF2 import PdfFileReader, PdfFileWriter

                                            ### CREATE INVOICE ###
def create_invoice(data, chain_name, chain_class, chain_specific_block=None):
    packet = io.BytesIO()
    chain = chain_class
    can = canvas.Canvas(packet, pagesize=letter)
    
    ### DRAW HEADER PART ###
    draw_invoice_header(can, data, chain)
    ### DRAW BODY PART ###
    draw_invoice_period(can, data)
    
    # this coorinates indicates start of the BODY part of the invoice
    y_start = 495
    y_gap = 12
    block_gap = 7
    x_start = 10
    # draw blocks with drug_type + basis_of_reimbursement_source combinations
    y_start = draw_drug_type_plus_price_source_combinations(can, data, x_start, y_start, y_gap, block_gap)
    
    if chain_specific_block:
        y_start = draw_chain_specific_block(can, chain_specific_block, x_start, y_start, y_gap, block_gap)
    
    ### DRAW FOOTER PART ###
    can.setDash([6, 2])
    can.setStrokeColor(grey)
    can.line(10, y_start + 15, 585, y_start + 15)
    
    can.setFont("Helvetica-Bold", 12)
    balance = data.loc[data['drug_type'] == 'brand'].iloc[0]['grand total']
    can.drawString(535, y_start - 2, f"${balance.strip()}")
    draw_payment_instructions(can, y_start - 2)
    can.drawString(300, y_start - 2, "BALANCE DUE")
    ### END OF DRAWING ###

    can.save()
    
    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    pdf_template = PdfFileReader(open("pdf/default.pdf", "rb"))

    output = PdfFileWriter()
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)

    output_file = f"Hippo_{chain_name}_Invoice_{data.iloc[0]['invoice_number']}_{data.iloc[0]['invoice_date'].replace('/', '')}.pdf"
    outputString = io.BytesIO()
    output.write(outputString)

    return outputString, output_file

                                            ### DRAW HEADER ###
def draw_invoice_header(can, data, chain):
    """
    print default invoice HEADER information like BILL TO:, INVOICE #, DATE, TERMS, DUE DATE
    """
    can.setFont("Helvetica", 9)
    can.drawString(468, 673, str(data.iloc[0]['invoice_number']))
    can.drawString(468, 660, str(data.iloc[0]['invoice_date']))
    can.drawString(468, 647, "NET " + chain.NET_NUMBER)
    can.drawString(468, 634, str(data.iloc[0]['due_date']))
    can.drawString(10, 660, chain.COMPANY_NAME)
    can.drawString(10, 647, chain.ADDR_LINE_2)
    if hasattr(chain, 'ADDR_LINE_3'):
        can.drawString(10, 634, chain.ADDR_LINE_3)
    if hasattr(chain, 'ADDR_LINE_4'):
        can.drawString(10, 621, chain.ADDR_LINE_4)

                                            ### DRAW BODY ###
def draw_invoice_period(can, data):
    can.setFont("Helvetica", 10)
    period_start = data.iloc[0]['period_start']
    period_end = data.iloc[0]['period_end']
    for_period = f"FOR PERIOD: {period_start} - {period_end}"
    can.drawString(10, 525, for_period)

def draw_drug_type_plus_price_source_combinations(can, data, x_start, y_start, y_gap, block_gap):
    """
    this function draw default block of information for each combination of 'drug_type'&'basis_of_reimbursement_source';
    UNC combinations excluded. i.e. GENERIC + AWP, GENERIC + NADAC, BRAND + AWP;
    coordinates for each block calculates dynamically.
    """
    combinations = data[data['basis_of_reimbursement_source'] != 'UNC'][['drug_type', 'basis_of_reimbursement_source']].drop_duplicates()
    
    for _, combo in combinations.iterrows():
        drug_type = combo['drug_type']
        basis_of_reimbursement_source = combo['basis_of_reimbursement_source']
        
        can.setFont("Helvetica-Bold", 10)
        can.drawString(x_start, y_start, f"{drug_type.upper()} - {basis_of_reimbursement_source}")
        can.setFont("Helvetica-Bold", 12)
        admin_fee_value_series = data[(data['drug_type'] == drug_type) & (data['basis_of_reimbursement_source'] == basis_of_reimbursement_source)]['total administration fee owed']
        draw_text_aligned(can, data['grand total'].iloc[0], 535, y_start, "$" + str(admin_fee_value_series.iloc[0]))
        y_start -= y_gap
        can.setFont("Helvetica", 10)

        items_with_labels = [
            # ('claims count', 'CLAIMS COUNT'), # comment this line until the end of the PR reviw. Not sure wether we need it in the invoice or not
            ('total paid claims', 'TOTAL PAID CLAIMS'), 
            ('total remunerative paid claims', 'TOTAL REMUNERATIVE PAID CLAIMS'),
            ('total ingredient cost paid', 'TOTAL INGREDIENT COST PAID'),
            ('total administration fee owed', 'TOTAL ADMINISTRATION FEE OWED')
        ]

        for item_name, label in items_with_labels:
            dollar = '$' if 'cost' in item_name or 'fee' in item_name else ''
            draw_item(data, can, x_start, y_start, item_name, drug_type, basis_of_reimbursement_source, label, dollar)
            y_start -= y_gap

        y_start -= block_gap

    return y_start

                                            ### DRAW FOOTER ###
def draw_payment_instructions(can, y_start):
    """
    draw PAYMENT INSTRUCTIONS (footer)
    """
    payment_instructions_string = """
        PAYMENT INSTRUCTIONS:
        Bank of America, N.A, 114 West 47th Street, 4th Floor, New York,
        NY 10036 Electronic ABA/Routing No.: 11500010
        Wire Transfer ABA/Routing No: 026009593
        Name of Subscriber Account: HIPPO TECHNOLOGIES LLC
        Credit Account No.: 394008084925
    """
    custom_grey = Color(0.7, 0.7, 0.7)
    can.setFont("Helvetica", 7)
    can.setFillColor(custom_grey)
    
    text_obj = can.beginText()
    text_obj.setTextOrigin(10, y_start)
    text_obj.textLines(payment_instructions_string)
    
    can.drawText(text_obj)


def draw_item(data, can, x_start, y_start, item_name, drug_type, basis_of_reimbursement_source, label, dollar):
    item_value = data.loc[
        (data['drug_type'] == drug_type) & 
        (data['basis_of_reimbursement_source'] == basis_of_reimbursement_source)
    ].iloc[0][item_name]
    can.drawString(x_start, y_start, f"{label}: {dollar}{item_value}")

def draw_text_aligned(canvas, grand_total_string, x_end, y, text):
    """
    this function dynamically calculate the x/y coordinate in order to allign admin_fee_owed value appropriately 
    grand_total_string - the total admeen fee owed by pharmacy value presented as a string
    x_end - x coordinate representing starting X position for grand total string
    y - y coordinate
    """
    # calculate the width of the grand total value in order to use this value as a reference for the aligning
    grand_total_width = canvas.stringWidth(grand_total_string, "Helvetica-Bold", 12)

    # calculate the width of the total_admin_fee value for current block
    text_width = canvas.stringWidth(text, "Helvetica", 10)
    # x coordinate representing the end position of the grand_total_value on canvas
    x_end = x_end + grand_total_width
    # calculate the x position based on the end position and the width of the text
    x_start = x_end - text_width
    canvas.setFont("Helvetica", 10)
    # Draw the text
    canvas.drawString(x_start, y, text)

def draw_chain_specific_block(can, data_dict, x_start, y_start, y_gap, block_gap):
    """
    Draws a labeled block of data (text + value) on the invoice PDF canvas.
    The label for the block comes from a special key in data_dict: "Chain specific label".
    Numeric values will be formatted with comma separators and 2 decimal places.
    """
    header_label = data_dict.pop("Chain specific label", "CHAIN-SPECIFIC DETAILS")
    can.setFont("Helvetica-Bold", 10)
    can.drawString(x_start, y_start, header_label.upper())
    y_start -= y_gap

    can.setFont("Helvetica", 10)

    for label, value in data_dict.items():
        label_str = str(label).upper()

        if isinstance(value, (float, Decimal)):
            value_str = f"{value:,.2f}"
            display_value = "$" + value_str
        else:
            value_str = str(value)
            display_value = value_str  # No $ prefix

        can.drawString(x_start, y_start, label_str)
        draw_text_aligned(can, value_str, 535, y_start, display_value)
        y_start -= y_gap

    y_start -= block_gap
    return y_start
