import datetime
import calendar
import random

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from io import BytesIO
from hippo import s3
from hippo.sources.s3 import S3Downloader
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from tasks import invoice_utils as iu
from reportlab.pdfgen import canvas
from PyPDF2 import PdfFileWriter, PdfFileReader


def draw_address_details(can):
    can.setFont("Helvetica", 8)
    can.drawString(10, 660, "GoodRx")
    can.drawString(10, 650, "2701 Olympic Boulevard")
    can.drawString(10, 640, "Santa Monica, California, 90404")


def draw_invoice_details(can, invoice_number, invoice_date, due_date):
    can.setFont("Helvetica", 9)
    can.drawString(448, 672, str(invoice_number))
    can.drawString(448, 660, str(invoice_date))
    can.drawString(448, 647, str(due_date))


def create_pdf(schedule, df, start_date, end_date, column_name, value_column):
    buffer = BytesIO()

    pdf_canvas_buffer = BytesIO()
    can = canvas.Canvas(pdf_canvas_buffer, pagesize=LETTER)
    draw_address_details(can)
    report_date = (end_date + datetime.timedelta(days=1)).strftime("%m/%d/%Y")
    if schedule.lower() == 'schedule b':
        due_date = end_date + relativedelta(months=12)
    elif end_date.day <= 15:
        due_date = end_date + relativedelta(months=1, day=15)
    else:
        due_date = end_date + relativedelta(months=1, day=31)
    due_date = due_date.strftime("%m/%d/%Y")
    draw_invoice_details(can, iu.get_invoice_number(30104, precise=True), report_date, due_date)
    can.save()

    pdf_canvas_buffer.seek(0)
    overlay_pdf = PdfFileReader(pdf_canvas_buffer)

    doc_buffer = BytesIO()
    doc = SimpleDocTemplate(
        doc_buffer,
        pagesize=LETTER,
        leftMargin=25,
        rightMargin=36,
        topMargin=200,
        bottomMargin=36
    )
    styles = getSampleStyleSheet()
    story = []

    body_style = ParagraphStyle(
        name='BodyStyle',
        parent=styles['Normal'],
        alignment=1,
        fontSize=9,
        leading=12,
    )

    table_data = [[
        Paragraph("<b>Schedule</b>", body_style),
        Paragraph("<b>Period Start</b>", body_style),
        Paragraph("<b>Period End</b>", body_style),
        Paragraph("<b>Fills</b>", body_style),
        Paragraph("<b>Reversals</b>", body_style),
        Paragraph("<b>Fills Less Reversals</b>", body_style),
        Paragraph(f"<b>{column_name}</b>", body_style),
    ]]

    for _, row in df.iterrows():
        table_data.append([
            str(row['schedule']),
            str(row['period_start']),
            str(row['period_end']),
            f"{row['fill_count']:,.0f}",
            f"{row['reversal_count']:,.0f}",
            f"{row['net_fills']:,.0f}",
            f"$ {row[value_column]:,.2f}",
        ])

    table = Table(table_data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.white),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 4),
        ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    ]))

    story.append(table)
    story.append(Spacer(1, 36))

    total_due = df[value_column].sum()
    centered_style = ParagraphStyle(
        name='CenteredStyle',
        fontName='Helvetica',
        fontSize=12,
        alignment=1
    )
    story.append(Paragraph(f"<b>Total Due:</b>              ${total_due:,.2f}", centered_style))

    doc.build(story)
    doc_buffer.seek(0)

    main_pdf = PdfFileReader(doc_buffer)
    template_pdf = PdfFileReader(open("pdf/default_no_bar.pdf", "rb"))

    output = PdfFileWriter()
    page = template_pdf.getPage(0)
    page.mergePage(main_pdf.getPage(0))
    page.mergePage(overlay_pdf.getPage(0))
    output.addPage(page)

    output.write(buffer)
    buffer.seek(0)

    return buffer


def calculate_invoice_period(input_date, test_run_flag):
    if input_date.day == 1:
        previous_month = input_date.month - 1 or 12
        year = input_date.year if input_date.month > 1 else input_date.year - 1
        start_date = datetime.datetime(year, previous_month, 16)
        last_day = calendar.monthrange(year, previous_month)[1]
        end_date = datetime.datetime(year, previous_month, last_day)
    elif input_date.day == 16:
        start_date = datetime.datetime(input_date.year, input_date.month, 1)
        end_date = datetime.datetime(input_date.year, input_date.month, 15)
    elif test_run_flag.lower() == 'true':
        if input_date.day >= 16:
            start_date = datetime.datetime(input_date.year, input_date.month, 16)
        else:
            start_date = datetime.datetime(input_date.year, input_date.month, 1)
        end_date = datetime.datetime(input_date.year, input_date.month, input_date.day-1)
    else:
        raise RuntimeError(
            "Current date isn't expected for POS invoice. Please set test_run_flag if you want to run it anyway"
        )

    return start_date, end_date


def try_parse_date(strdate):
    """
    try to parse date by applying '%Y-%m-%d' and then '%Y.%m.%d' templates
    fail loudly
    """
    templates = ['%Y-%m-%d', '%Y.%m.%d']
    if not strdate or strdate == 'None':
        return datetime.datetime.utcnow()
    for template in templates:
        try:
            date = datetime.datetime.strptime(strdate, template)
            return date.date()
        except:
            pass
    raise Exception(f"could not parse '{strdate}' using {templates} as templates")


def _read_pos_feed_files(goodrx_bucket, pos_feed_prefix, start_period, end_period):
    schedule_a_feeds = []
    schedule_b_feeds = []
    current_date = start_period + datetime.timedelta(days=1)
    all_feed_files_prefix = s3.dir(goodrx_bucket, pos_feed_prefix)
    past_feeds = []
    for prefix in all_feed_files_prefix:
        prefix_date = prefix.split('/')[2]
        if datetime.datetime.strptime(prefix_date, "%Y.%m.%d") < current_date:
            past_feeds.append(
                pd.read_csv(f"s3://{goodrx_bucket}/{prefix}", dtype=object)
            )
    while current_date <= (end_period + datetime.timedelta(days=1)):
        prefix = f"{pos_feed_prefix.rstrip('/')}/{current_date.strftime('%Y.%m.%d')}"
        files_in_prefix = s3.dir(goodrx_bucket, prefix)
        if not files_in_prefix:
            raise RuntimeError(f'POS Feed files not found in the prefix {prefix}')
        for file_name in files_in_prefix:
            if 'dcaw_' in file_name:
                schedule_a_feeds.append(
                    pd.read_csv(f"s3://{goodrx_bucket}/{file_name}", dtype=object)
                )
            elif 'rb_' in file_name:
                schedule_b_feeds.append(
                    pd.read_csv(f"s3://{goodrx_bucket}/{file_name}", dtype=object)
                )
        current_date += datetime.timedelta(days=1)
    all_schedule_a_claims = pd.concat(schedule_a_feeds)
    all_schedule_b_claims = pd.concat(schedule_b_feeds)
    all_fills_claims = pd.concat(past_feeds + schedule_a_feeds + schedule_b_feeds)
    all_fills_claims = all_fills_claims[all_fills_claims['claim_reference_id'].isna()]
    all_schedule_a_claims['claim_timestamp'] = pd.to_datetime(
        all_schedule_a_claims['claim_timestamp'], format='%Y-%m-%d %H:%M:%S.%f'
    )
    all_schedule_b_claims['claim_timestamp'] = pd.to_datetime(
        all_schedule_b_claims['claim_timestamp'], format='%Y-%m-%d %H:%M:%S.%f'
    )
    all_fills_claims['claim_timestamp'] = pd.to_datetime(
        all_fills_claims['claim_timestamp'], format='%Y-%m-%d %H:%M:%S.%f'
    )
    return all_schedule_a_claims, all_schedule_b_claims, all_fills_claims


def calculate_invoice_data(feeds, start_period, end_period):
    feeds['plan_pay'] = feeds['plan_pay'].astype('float')
    feeds['copay_amount'] = feeds['copay_amount'].astype('float')
    feeds['drug_quantity'] = feeds['drug_quantity'].astype('float')
    feeds['fill_plan_pay'] = np.where(feeds['plan_pay'] < 0, -feeds['plan_pay'], feeds['plan_pay'])
    feeds['profit_share'] = np.where(
        feeds['payout_type'].eq('buydown_price'),
        feeds['copay_amount'] - (feeds['drug_quantity'] * feeds['price_per_unit']),
        np.where(
            feeds['payout_type'].eq('fixed_buydown'),
            feeds['copay_amount'] - (feeds['payout_price']),
            (feeds['drug_quantity'] * feeds['price_per_unit']) - feeds['fill_plan_pay']
        ),
    )
    feeds['profit_share'] = feeds['profit_share'] * 0.45
    feeds['profit_share'] = np.where(feeds['claim_reference_id'].isna(), feeds['profit_share'], -feeds['profit_share'])

    fills = feeds[feeds['claim_reference_id'].isnull()].copy()
    fills['fill_count'] = 1
    fills['reversal_count'] = 0
    fills = fills[~fills['claim_id'].isin(feeds['claim_reference_id'])]
    fills = fills.agg(
        plan_pay=('plan_pay', 'sum'),
        profit_share=('profit_share', 'sum'),
        fill_count=('fill_count', 'sum'),
        reversal_count=('reversal_count', 'sum')
    ).reset_index(drop=True)

    reversals = feeds[~feeds['claim_reference_id'].isnull()].copy()
    reversals['fill_count'] = 0
    reversals['reversal_count'] = 1
    reversals = reversals[~reversals['claim_reference_id'].isin(feeds['claim_id'])]
    reversals = reversals.agg(
        plan_pay=('plan_pay', 'sum'),
        profit_share=('profit_share', 'sum'),
        fill_count=('fill_count', 'sum'),
        reversal_count=('reversal_count', 'sum')
    ).reset_index(drop=True)

    combined = pd.concat([fills, reversals], ignore_index=True).sum()
    combined['net_fills'] = combined['fill_count'] - combined['reversal_count']
    combined['period_start'] = start_period.strftime('%-d-%b-%Y')
    combined['period_end'] = end_period.strftime('%-d-%b-%Y')
    return combined.to_frame().T


def process_historical_ndc_config(feed_df, brand_channel_ndc_config_history, all_past_feed_claims):
    merged_df = feed_df.merge(
        brand_channel_ndc_config_history.rename(columns={'brand_channel_name': 'channel'}),
        on=['ndc', 'channel'],
        how='inner'
    )

    merged_df = merged_df.merge(
        all_past_feed_claims[['claim_id', 'date_filled']].rename(columns={
            'claim_id': 'fill_id',
            'date_filled': 'fill_timestamp'
        }),
        how='left', left_on='claim_reference_id', right_on='fill_id',
    )
    merged_df['claim_timestamp_dos'] = pd.to_datetime(
        merged_df['date_filled'], format='%Y%m%d'
    ) + pd.Timedelta(hours=12)
    merged_df['fill_timestamp'] = pd.to_datetime(
        merged_df['fill_timestamp'], format='%Y%m%d'
    ) + pd.Timedelta(hours=12)

    merged_df['drug_quantity'] = pd.to_numeric(merged_df['drug_quantity'])
    merged_df['drug_days_supply'] = pd.to_numeric(merged_df['drug_days_supply'])

    filtered_df = merged_df[
        (
            (merged_df['claim_status'].eq('reversed') & (merged_df['fill_timestamp'] >= merged_df['valid_from'])) |
            (~merged_df['claim_status'].eq('reversed') & (merged_df['claim_timestamp_dos'] >= merged_df['valid_from']))
        ) &
        (
            (merged_df['claim_status'].eq('reversed') & (merged_df['fill_timestamp'] <= merged_df['valid_to'])) |
            (~merged_df['claim_status'].eq('reversed') & (merged_df['claim_timestamp_dos'] <= merged_df['valid_to']))
        ) &
        (merged_df['drug_quantity'] <= merged_df['max_quantity']) &
        (merged_df['drug_quantity'] >= merged_df['min_quantity']) &
        (merged_df['drug_days_supply'] >= merged_df['min_ds']) &
        (merged_df['drug_days_supply'] <= merged_df['max_ds'])
    ]

    assert filtered_df.shape[0] == feed_df.shape[0]
    assert filtered_df[filtered_df['claim_id'].duplicated()].empty, \
        'Feed join with brand_channel_ndc_config_history generated duplicates. Please investigate'

    return filtered_df


def run(
    invoice_bucket, goodrx_bucket, pos_feed_prefix, goodrx_historical_prefix, manual_date_string, test_run_flag,
    slack_channel, slack_bot_token, hsdk_env, **kwargs
):
    run_date = try_parse_date(manual_date_string)
    start_period, end_period = calculate_invoice_period(run_date, test_run_flag)
    schedule_a_feed, schedule_b_feed, all_past_fill_claims = _read_pos_feed_files(goodrx_bucket, pos_feed_prefix, start_period, end_period)
    ndc_config_history = _read_ndc_config_historical(goodrx_bucket, goodrx_historical_prefix)

    schedule_a_feed = process_historical_ndc_config(schedule_a_feed, ndc_config_history, all_past_fill_claims)
    schedule_b_feed = process_historical_ndc_config(schedule_b_feed, ndc_config_history, all_past_fill_claims)

    schedule_a_summary = calculate_invoice_data(schedule_a_feed, start_period, end_period)
    schedule_a_summary['schedule'] = 'schedule A'

    schedule_b_summary = calculate_invoice_data(schedule_b_feed, start_period, end_period)
    schedule_b_summary['schedule'] = 'schedule B'

    final_summary = pd.concat([schedule_a_summary, schedule_b_summary])
    report_date = (end_period + datetime.timedelta(days=1)).strftime("%m%d%Y")

    pharmacy_summary = final_summary[[
        'schedule', 'period_start', 'period_end', 'fill_count', 'reversal_count', 'net_fills', 'plan_pay',
    ]]
    file_name = f'Hippo_GoodRx_POS_Statement_{report_date}_pharmacy.pdf'
    file_bytes = create_pdf(
        'pharmacy', pharmacy_summary, start_period, end_period, column_name='Total Plan Pay',
        value_column='plan_pay'
    )

    iu.send_file(
        pharmacy_summary, "GoodRx", file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket,
        hsdk_env=hsdk_env, test_run_flag=test_run_flag, **kwargs
    )

    profit_summary = final_summary[[
        'schedule', 'period_start', 'period_end', 'fill_count', 'reversal_count', 'net_fills', 'profit_share',
    ]]
    for schedule, schedule_group in profit_summary.groupby('schedule'):
        file_name = f"Hippo_GoodRx_POS_Statement_{schedule.replace(' ', '_').title()}_{report_date}_profit.pdf"
        file_bytes = create_pdf(schedule, schedule_group, start_period, end_period, column_name='Profit Share',
                                value_column='profit_share')
        iu.send_file(
            schedule_group, "GoodRx", file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket,
            hsdk_env=hsdk_env, test_run_flag=test_run_flag, **kwargs
        )


def _read_ndc_config_historical(goodrx_bucket, goodrx_historical_prefix):
    ndc_config_history = S3Downloader(goodrx_bucket, goodrx_historical_prefix, 'brand_channel_ndc_config_v2').pull()[0]
    ndc_config_history['valid_to'] = ndc_config_history['valid_to'].str.replace('2300.01.01', '2100.01.01', regex=False)
    ndc_config_history['valid_to'] = pd.to_datetime(ndc_config_history['valid_to'], format='%Y-%m-%d')
    ndc_config_history['valid_from'] = pd.to_datetime(ndc_config_history['valid_from'], format='%Y-%m-%d')
    ndc_config_history['min_quantity'] = pd.to_numeric(ndc_config_history['min_quantity'].fillna('0'))
    ndc_config_history['min_ds'] = pd.to_numeric(ndc_config_history['min_ds'].fillna('0'))
    ndc_config_history['max_quantity'] = pd.to_numeric(ndc_config_history['max_quantity'].fillna('9999999'))
    ndc_config_history['max_ds'] = pd.to_numeric(ndc_config_history['max_ds'].fillna('9999999'))
    ndc_config_history['payout_price'] = pd.to_numeric(ndc_config_history['payout_price'])/100
    ndc_config_history['default_quantity'] = pd.to_numeric(ndc_config_history['default_quantity'])
    ndc_config_history['price_per_unit'] = ndc_config_history['payout_price'] / ndc_config_history['default_quantity']
    return ndc_config_history


