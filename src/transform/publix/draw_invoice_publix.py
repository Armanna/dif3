from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from reportlab.lib.colors import Color
from io import BytesIO
import pandas as pd
from PyPDF2 import PdfFileReader, PdfFileWriter
import io

def draw_invoice_header(can, data, chain):
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


def draw_payment_instructions(can, y_start):
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
    can.setFillColorRGB(0, 0, 0)

def draw_invoice_period(can, data):
    can.setFont("Helvetica", 10)
    period_start = data.iloc[0]['period_start']
    period_end = data.iloc[0]['period_end']
    for_period = f"FOR PERIOD: {period_start} - {period_end}"
    can.drawString(10, 590, for_period)

def create_invoice(df, chain_name, chain_class, business_labels=("Business A", "Business B")):
    packet = io.BytesIO()
    c = canvas.Canvas(packet, pagesize=LETTER)
    width, height = LETTER

    y_start = height - 60
    invoice_number = df['invoice_number'].iloc[0].astype('int')
    invoice_date = df['invoice_date'].iloc[0]
    due_date = df['due_date'].iloc[0]
    period_start = df['period_start'].iloc[0]
    period_end = df['period_end'].iloc[0]

    header_data = pd.DataFrame.from_dict([{
        'invoice_number': invoice_number,
        'invoice_date': invoice_date,
        'due_date': due_date,
        'period_start': period_start,
        'period_end': period_end
    }])

    draw_invoice_header(c, header_data, chain_class)
    draw_invoice_period(c, header_data)

    y = 525
    margin = 10
    col1_x = margin + 140
    col2_x = margin + 310
    col3_x = margin + 470

    c.setFont("Helvetica-Bold", 10)
    c.drawString(col1_x, y, business_labels[0])
    c.drawString(col2_x, y, business_labels[1])
    c.drawString(col3_x, y, "Total Admin Fee")
    y -= 15
    c.line(margin, y, width - margin, y)
    y -= 15

    def format_currency(val):
        return f"${val:,.2f}" if pd.notna(val) else ""

    def parse_numeric_column(col):
        return pd.to_numeric(col.astype(str).str.replace(',', ''), errors='coerce').fillna(0)

    for _, row in df.iterrows():
        title = f"{row['drug_type'].capitalize()} - {row['basis_of_reimbursement_source']}"
        c.setFont("Helvetica-Bold", 10)
        c.drawString(margin, y, title)
        y -= 15

        c.setFont("Helvetica", 10)
        metrics = [
            ("Total Paid Claims", 'total paid claims'),
            ("Total Remunerative Paid Claims", 'total remunerative paid claims'),
            ("Total Ingredient Cost Paid", 'total ingredient cost paid'),
            ("Total Administration Fee Owed", 'total administration fee owed'),
        ]

        for label, col_base in metrics:
            left_val = row.get(f"{col_base}_regular", '')
            right_val = row.get(f"{col_base}_marketplace", '')
            admin_val = row.get('total administration fee owed_regular', 0)
            admin_val2 = row.get('total administration fee owed_marketplace', 0)

            if 'cost' in col_base or 'fee' in col_base:
                # Format as currency
                try:
                    left_val = format_currency(float(str(left_val).replace(',', '')))
                except:
                    left_val = ''
                try:
                    right_val = format_currency(float(str(right_val).replace(',', '')))
                except:
                    right_val = ''
            elif 'claims' in col_base:
                # Treat as integer counts
                try:
                    left_val = str(int(float(str(left_val).replace(',', ''))))
                except:
                    left_val = '0'
                try:
                    right_val = str(int(float(str(right_val).replace(',', ''))))
                except:
                    right_val = '0'
            else:
                left_val = str(left_val) if pd.notna(left_val) else ''
                right_val = str(right_val) if pd.notna(right_val) else ''

            admin_total = 0.0
            try:
                admin_total = float(str(admin_val).replace(',', '')) + float(str(admin_val2).replace(',', ''))
            except:
                admin_total = 0.0

            c.drawString(margin, y, f"{label}:")
            c.drawRightString(col1_x + 80, y, left_val)
            c.drawRightString(col2_x + 80, y, right_val)
            if label == "Total Administration Fee Owed":
                c.drawRightString(col3_x + 60, y, format_currency(admin_total))
            y -= 15

        y -= 10
        if y < 150:
            c.showPage()
            y = y_start

    fee_col1 = parse_numeric_column(df['total administration fee owed_regular'])
    fee_col2 = parse_numeric_column(df['total administration fee owed_marketplace'])
    balance_due = fee_col1.sum() + fee_col2.sum()

    y -= 20
    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin, y, "BALANCE DUE:")
    c.drawRightString(width - margin - 42, y, format_currency(balance_due))
    draw_payment_instructions(c, y - 12)
    c.save()

    packet.seek(0)
    new_pdf = PdfFileReader(packet)
    pdf_template = PdfFileReader(open("pdf/default.pdf", "rb"))
    output = PdfFileWriter()
    page = pdf_template.getPage(0)
    page.mergePage(new_pdf.getPage(0))
    output.addPage(page)
    for page_num in range(1, new_pdf.getNumPages()):
        output.addPage(new_pdf.getPage(page_num))
    output_buffer = BytesIO()
    output.write(output_buffer)
    output_buffer.seek(0)

    return output_buffer
