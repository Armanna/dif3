import pandas as pd
import numpy as np
import io
import datetime
from datetime import timedelta, datetime
from decimal import Decimal, ROUND_HALF_EVEN
from datetime import datetime as dt

from hippo.exporters import Registry
from hippo.exporters import s3 as s3_exporter
from hippo.exporters import fs as fs_exporter

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from hippo import logger

log = logger.getLogger('utils.py')

def process_invoice_dates(period_flag, manual_date_str = None):
  today = datetime.strptime(manual_date_str, "%Y-%m-%d") if manual_date_str not in [None, 'None'] else dt.now()
  if period_flag == 'bi-weekly':
    if today.day > 15:
      period_start = dt(today.year, today.month, 1)
      period_end = dt(today.year, today.month, 1) + pd.Timedelta(days=14)
    else:
      if today.month == 1: # need this block to avoid errors during 1st month of the year
        period_start = dt(today.year - 1, 12, 16)
        period_end = dt(today.year, today.month, 1) - timedelta(days=1)
      else:
        period_start = dt(today.year, today.month - 1, 16)
        period_end = dt(today.year, today.month, 1) - timedelta(days=1)
  elif period_flag == 'quarter':
    quarter = (today.month - 1) // 3
    period_start = datetime(today.year, 3 * quarter - 2, 1) if quarter > 0 else datetime(today.year - 1, 10, 1)
    period_end = period_start.replace(month=period_start.month + 3, day=1) - timedelta(days=1) if quarter > 0 else datetime(today.year - 1, 12, 31)
  elif period_flag == 'month':
    period_start = dt(today.year, today.month - 1, 1) if today.month != 1 else dt(today.year - 1, 12, 1)
    period_end = dt(today.year, today.month, 1) - timedelta(days=1) if today.month != 1 else dt(today.year - 1, 12, 31)
  elif period_flag == 'year':
    period_start = dt(today.year - 1, 1, 1)
    period_end = dt(today.year - 1, 12, 31)
  elif period_flag == 'day': # mostly need if for test puprose when we need to download a sample of data
    period_start = dt(today.year, today.month, today.day - 1)
    period_end = dt(today.year, today.month, today.day - 1)
  return period_start, period_end

def cast_columns_to_decimal(df, column_names, fillna_flag=False):
    for column in column_names:
        if column in df.columns:
            if fillna_flag:
                df[column] = df[column].fillna(0).apply(lambda x: Decimal(str(x)))
            else:
                df[column] = df[column].apply(lambda x: Decimal(str(x)) if pd.notna(x) else x)
    return df

def apply_formatting(df, columns, formatting):
  df[columns] = df[columns].applymap(lambda x: formatting.format(x))
  return df

def cast_cents_to_dollars(df, column_names, add_fill_reversal_sign=True):
    if isinstance(column_names, dict):
        for new_column_name, column in column_names.items():
            if column in df.columns:
                df[new_column_name] = df[column] * df.fill_reversal_indicator / Decimal('100') if add_fill_reversal_sign else df[column] / Decimal('100')
        return df
    elif isinstance(column_names, str):
        if column_names in df.columns:
            df[column_names] = df[column_names] * df.fill_reversal_indicator / Decimal('100') if add_fill_reversal_sign else df[column_names] / Decimal('100')
        return df[column_names]
    elif isinstance(column_names, list):
        for column in column_names:
            if column in df.columns:
                df[column] = df[column] * df.fill_reversal_indicator / Decimal('100') if add_fill_reversal_sign else df[column] / Decimal('100')
        return df

def transactions_file_name(chain_name, report_type='invoice_utilization', period_start=''):
    if report_type == 'invoice_utilization':
        return f"{period_start}Hippo_" + chain_name + "_Utilization_" + dt.today().strftime("%m%d%Y") + ".xlsx"
    if report_type == 'quarterly':
        return f"{period_start}Hippo_" + chain_name + "_Quarterly_Transactions_" + dt.today().strftime("%m%d%Y") + ".xlsx"
    if report_type == 'summary':
        return f"{period_start}Hippo_" + chain_name + "_Quarterly_Summary_" + dt.today().strftime("%m%d%Y") + ".xlsx"
    return f"{period_start}Hippo_" + chain_name + f"_{report_type}_" + dt.today().strftime("%m%d%Y") + ".xlsx"


def format_decimal(x):
    x = Decimal(str(x)).quantize(Decimal('.0001'), rounding=ROUND_HALF_EVEN)
    if x == int(x):
        return int(x)
    else:
        return x

def calculate_ger_and_ber_metrics(df, process_reversals_flag=True):
    if process_reversals_flag:
        mask = df['Excluded Claim Indicator'] == 'N'
    else:
        mask = (df['Excluded Claim Indicator'] == 'N') & (df['reversal indicator'] != 'B2')

    df = df[mask]
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby(['drug_type', 'reconciliation_price_basis', 'target_rate', 'dfer_target_rate'], as_index=False).agg({
        'authorization_number': 'count',
        'contracted_cost': 'sum',
        'full_cost': 'sum',
        'ingredient_cost_paid_resp': 'sum',
        'dispensing_fee_paid_resp': 'sum',
        'total_paid_response': 'sum'
    }).rename(columns={'authorization_number': 'Total claims'})

    grouped = grouped[grouped['Total claims'] > 0]
    grouped['Actual rate'] = ((1 - grouped['ingredient_cost_paid_resp'] / grouped['full_cost']) * Decimal('100')).apply(lambda x: x.quantize(Decimal('.01')))
    grouped['Ingr Cost Dollar variance'] = (grouped['ingredient_cost_paid_resp'] - grouped['contracted_cost']).apply(lambda x: x.quantize(Decimal('.01')))
    grouped['Contracted Dispensing Fee'] = grouped['Total claims'] * grouped['dfer_target_rate']
    grouped['Dispensing fee variance'] = (grouped['dispensing_fee_paid_resp'] - grouped['Contracted Dispensing Fee']).apply(lambda x: x.quantize(Decimal('.01')))

    return grouped[['drug_type', 'reconciliation_price_basis', 'Total claims', 'contracted_cost', 'full_cost',
                    'ingredient_cost_paid_resp', 'Actual rate', 'target_rate', 'dfer_target_rate',
                    'dispensing_fee_paid_resp', 'Contracted Dispensing Fee', 'Dispensing fee variance',
                    'Ingr Cost Dollar variance', 'total_paid_response']]

def create_report_dict(grouped_df, dispensing_fee_flag=False):
    price_basis_list = grouped_df['reconciliation_price_basis'].unique()
    df_dict = {}

    # Define default ger/ber and dispensing fee metrics
    report_metrics = [
        'Total claims', 
        'Contracted {}', 
        'Total {}', 
        'Total Ingredient Cost Paid', 
        'Contracted {} Rate', 
        'Actual Rate', 
        '{} Variant Amount'
    ]

    dispensing_fee_metrics = [
        'Total claims',
        'Contracted Dispensing Fee',
        'Actual Dispensing Fee Paid',
        'Dispense Fee Variant Amount'
    ]

    for price_basis in price_basis_list:
        price_basis_data = grouped_df[grouped_df['reconciliation_price_basis'] == price_basis]
        report_df = pd.DataFrame(columns=['Brand', 'Generic', 'Total Variant Amount'])
        
        # process default ger/ber block
        for metric in report_metrics:
            report_df.loc[metric.format(price_basis)] = [0, 0, '']

        for _, row in price_basis_data.iterrows():
            drug_type = row['drug_type'].capitalize()
            report_df.at['Total claims', drug_type] = row['Total claims']
            report_df.at[f'Contracted {price_basis}', drug_type] = row['contracted_cost']
            report_df.at[f'Total {price_basis}', drug_type] = row['full_cost']
            report_df.at['Total Ingredient Cost Paid', drug_type] = row['ingredient_cost_paid_resp']
            report_df.at[f'Contracted {price_basis} Rate', drug_type] = row['target_rate'] / Decimal('100')
            report_df.at['Actual Rate', drug_type] = row['Actual rate'] / Decimal('100')
            report_df.at[f'{price_basis} Variant Amount', drug_type] = row['contracted_cost'] - row['ingredient_cost_paid_resp']

        report_df.at[f'{price_basis} Variant Amount', 'Total Variant Amount'] = (
            report_df.at[f'{price_basis} Variant Amount', 'Brand'] + 
            report_df.at[f'{price_basis} Variant Amount', 'Generic']
        )

        df_dict[f'{price_basis} Report'] = report_df

        # process dispensing fee block
        if dispensing_fee_flag:
            dispense_fee_df = pd.DataFrame(columns=['Brand', 'Generic', 'Total Variant Amount'])
            for metric in dispensing_fee_metrics:
                dispense_fee_df.loc[metric] = [0, 0, '']

            for _, row in price_basis_data.iterrows():
                drug_type = row['drug_type'].capitalize()
                dispense_fee_df.at['Total claims', drug_type] = row['Total claims']
                dispense_fee_df.at['Contracted Dispensing Fee', drug_type] = row['Contracted Dispensing Fee']
                dispense_fee_df.at['Actual Dispensing Fee Paid', drug_type] = row['dispensing_fee_paid_resp']
                dispense_fee_df.at['Dispense Fee Variant Amount', drug_type] = row['Dispensing fee variance']

            dispense_fee_df.at['Dispense Fee Variant Amount', 'Total Variant Amount'] = (
                dispense_fee_df.at['Dispense Fee Variant Amount', 'Brand'] + 
                dispense_fee_df.at['Dispense Fee Variant Amount', 'Generic']
            )

            df_dict[f'{price_basis} Claims'] = dispense_fee_df

    # process TOTAL VARIANT AMOUNT block
    total_variant_amount_df = pd.DataFrame(columns=['Brand', 'Generic'])
    for price_basis in price_basis_list:
        report_variant = df_dict[f'{price_basis} Report'].at[f'{price_basis} Variant Amount', 'Total Variant Amount']
        if dispensing_fee_flag:
            report_variant += df_dict[f'{price_basis} Claims'].at['Dispense Fee Variant Amount', 'Total Variant Amount']

        total_variant_amount_df.loc[price_basis] = [report_variant, 0]

    total_variant_sum = total_variant_amount_df.sum()
    final_variant_amount_df = pd.DataFrame(
        {'Generic': [total_variant_sum['Generic']], 'Brand': [total_variant_sum['Brand']], 'Total': [total_variant_sum.sum()]},
        index=['TOTAL VARIANT AMOUNT']
    )

    df_dict['TOTAL VARIANT AMOUNT'] = final_variant_amount_df

    # process AFER block
    afer_value = grouped_df['total_paid_response'].sum() / grouped_df['Total claims'].sum()
    df_dict['AFER'] = pd.DataFrame({'AFER': [afer_value]})

    return df_dict

def write_quarterly_reconciliation_to_excel(report_dict, date_period, dispensing_fee_report_flag=False, afer_flag=False):
    output = io.BytesIO()
    
    def format_cell_formats(workbook):
        return {
            'header': workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1}),
            'bold': workbook.add_format({'bold': True, 'border': 1}),
            'currency': workbook.add_format({'num_format': '$#,##0.00', 'border': 1}),
            'negative_currency': workbook.add_format({'num_format': '$#,##0.00', 'font_color': 'red', 'border': 1}),
            'percent': workbook.add_format({'num_format': '0.00%', 'border': 1}),
            'integer': workbook.add_format({'num_format': '#,##0', 'border': 1}),
            'text': workbook.add_format({'border': 1}),
        }

    def write_report_block(worksheet, df, start_row, title, formats):
        worksheet.merge_range(start_row, 0, start_row, len(df.columns), title, formats['header'])
        start_row += 1
        
        worksheet.write_row(start_row, 0, [""] + list(df.columns), formats['bold'])
        start_row += 1

        for idx, row_data in enumerate(df.itertuples(index=False), start=start_row):
            label = df.index[idx - start_row]

            # Originally for Meijer, but we can merge the brand / generic cells when needed adding '(mergel_cells)' to
            # a given row index value in the report, so the writer will merge the cells
            if '(merge_cells)' in label:
                label = label.replace('(merge_cells)', '')
                value = row_data[1]
                worksheet.write(idx, 0, label, formats['text'])  # Label column
                cell_format = workbook.add_format({'num_format': '$#,##0.00', 'border': 1, 'align': 'center'})
                worksheet.merge_range(idx, 1, idx, len(df.columns) - 1, value, cell_format)
                continue

            worksheet.write(idx, 0, label, formats['text'])  # Label column
            for col_idx, value in enumerate(row_data):
                cell_format = formats['currency']
                
                if label == 'Total claims':
                    cell_format = formats['integer']
                elif label in ['Actual Rate', f'Contracted {title.split()[0]} Rate']:
                    cell_format = formats['percent']
                elif isinstance(value, (int, float)) and value < 0:
                    cell_format = formats['negative_currency']
                
                worksheet.write(idx, col_idx + 1, value, cell_format)
                
        return idx + 1  # Return the row index after writing

    with pd.ExcelWriter(output, engine='xlsxwriter', options={'nan_inf_to_errors': True}) as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Summary")

        formats = format_cell_formats(workbook)

        # Write the period string at the top
        worksheet.merge_range(0, 0, 0, 3, date_period, formats['header'])
        row = 2
        
        # Write main report blocks (WAC, NADAC, AWP Reports)
        for key, df in report_dict.items():
            if 'Report' in key and 'Claims' not in key:
                row = write_report_block(worksheet, df, row, key, formats)
                row += 1  # Add an empty line

        # Write DISPENSING FEE REPORT blocks if the flag is true
        if dispensing_fee_report_flag:
            worksheet.merge_range(row, 0, row, 3, "DISPENSING FEE REPORT", formats['header'])
            row += 2
            for key, df in report_dict.items():
                if 'Claims' in key:
                    row = write_report_block(worksheet, df, row, key, formats)
                    row += 1  # Add an empty line

        # Write TOTAL VARIANT AMOUNT block
        if 'TOTAL VARIANT AMOUNT' in report_dict:
            row += 1
            df = report_dict['TOTAL VARIANT AMOUNT']
            worksheet.write(row, 0, 'TOTAL VARIANT AMOUNT', formats['bold'])
            worksheet.write_row(row, 1, df.iloc[0].tolist(), formats['currency'])
            row += 1

        # Write AFER block if the flag is true
        if afer_flag:
            for key in ['AFER', 'Target AFER', 'AFER Diff']:
                if key in report_dict:
                    row += 1
                    df = report_dict[key]
                    worksheet.write(row, 0, key, formats['bold'])
                    worksheet.write_row(row, 1, df.iloc[0].tolist(), formats['currency'])

        # Adjust column widths
        for col_idx, col_name in enumerate(df.columns):
            max_len = max(df[col_name].astype(str).str.len().max(), len(col_name)) + 2
            worksheet.set_column(col_idx, col_idx, max_len)

        writer.close()
        output.seek(0)
        return output

def validate_price_basis(df, period_start, period_end, bucket, chain_name):
    df = df[df['price_basis'] != df['reconciliation_price_basis']]
    
    if df.empty == False:
        log.info("Incorrect price basis spotted. Dataframe will be uploaded to s3:/hippo-invoices-${HSDK_ENV_ID}/alerts/{chain name}/{report period}/")
        log.info(df[['price_basis', 'reconciliation_price_basis', 'product_id', 'authorization_number', 'drug_type', 'valid_from', 'valid_to', 'claim_date_of_service']].head(5))
        period_end_str = period_end.strftime('%Y-%m-%d')
        period_start_str = period_start.strftime('%Y-%m-%d')
        download_command = (
            f"\nDownload command:\n"
            "```\n"
            f"aws s3 cp s3://{bucket}/alerts/{chain_name}/{period_start_str}_{period_end_str}/price_basis_validation.csv /tmp/alerts/{chain_name}/{period_start_str}_{period_end_str}/\n"
            "```\n"
        )  
        log.info(download_command)

        export = Registry()\
        .add_exporter('fs', fs_exporter.FSExporter("/tmp")) \
        .add_exporter('s3', s3_exporter.S3Exporter(
            bucket,
            f"alerts/{chain_name}/{period_start_str}_{period_end_str}/",
            enabled=True,
        ))

        export.emit(f'price_basis_validation', df)

        msg_header = f":rotating_light::rotating_light: Invoices data require attention. Chain: {chain_name}. Period: {period_start_str} - {period_end_str} :rotating_light::rotating_light:\n"
        number_of_rows_msg = f"Price basis and Reconciliation price basis are not equal for {df.shape[0]} rows.\n"
        message = msg_header + number_of_rows_msg + download_command
        result_dict = {
            'dataframe': df,
            'message': message
        }
    else:
        log.info("No discrepancies found between Reconciliation and Actual Price basis")
        result_dict = {'dataframe': df, 'message': ''}
    return result_dict


def send_text_message_to_slack(slack_channel, slack_bot_token, text_msg):
  # send text message to the specific slack channel
  client = WebClient(token=slack_bot_token)

  try:
      result = client.chat_postMessage(
          channel=slack_channel,
          text=text_msg
      )
      log.info(result)

  except SlackApiError as e:
      log.info(f"Error sending message to Slack: {e.response['error']}")

def send_message_with_dataframe_to_slack(slack_channel, slack_bot_token, text_msg, dataframe=None, filename="data.csv"):
    client = WebClient(token=slack_bot_token)
    
    try:
        result = client.chat_postMessage(
            channel=slack_channel,
            text=text_msg
        )
        log.info("Message sent successfully.")
        log.info(result)
        
        if dataframe is not None:
            csv_buffer = io.BytesIO()
            dataframe.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            upload_result = client.files_upload(
                channels=slack_channel,
                file=csv_buffer,
                filename=filename,
                title="Attached CSV File"
            )
            log.info("DataFrame uploaded successfully as a CSV.")
            log.info(upload_result)

    except SlackApiError as e:
        log.error(f"Error interacting with Slack: {e.response['error']}")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
