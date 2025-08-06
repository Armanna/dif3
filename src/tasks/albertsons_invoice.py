#!/usr/bin/env python3
import io

import numpy as np
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from sources import claims as sources_claims, pharmacies
from transform import claims as transform_claims, draw_invoice, utils
from tasks import download_other_sources
from hippo import logger
from hippo.sources.s3 import S3Downloader
from . import invoice_utils as iu

log = logger.getLogger('Processing Albertsons invoice') 

chain_name = 'albertsons'
chain_class = pharmacies.Albertsons

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, period, manual_date_string, **kwargs):
    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")
    log.info(f"Running {period}ly report.")
    log.info(f"Running date is {manual_date_string}.")
    temp_main_prefix = f"temp/sources/albertsons/{period}/"
    temp_claims_prefix = f"temp/claims/albertsons/{period}"
    
    period_start, period_end = utils.process_invoice_dates(period_flag='bi-weekly', manual_date_str=manual_date_string)
    # manually handle exclusion for April 2025 because we oficially started processing Albertsons claims within direct deal at 1st of april
    period_start = pd.to_datetime('2025-04-01') if period_end == pd.to_datetime('2025-04-30') else period_start

    log.info(f"Downloading the data for the period: {period_start} - {period_end}.")
    raw_claims_df = S3Downloader(invoice_bucket, temp_claims_prefix, 'claims.parquet').pull_parquet()[0]
    claims_df = _preprocess_claims_df(raw_claims_df, period_start, period_end)
    log.info(f"Processing default source dictionary.")
    source_dict = download_other_sources.load_parquet_files_to_dict(invoice_bucket, temp_main_prefix)

                                        ### PROCESS TRANSACTIONS ###
    transaction_results = transform_claims.build_default_transactions_dataset(claims_df, source_dict, chain_name, chain_class, period_start, period_end)
    transaction_final_df = _select_fields_for_exporting(transaction_results)
    transaction_file_name = utils.transactions_file_name(chain_name=chain_name.upper())

                                        ### VALIDATE PRICE BASIS ###
    # need this validation in order to ensure that we don't blindly reconcile agains reconciliation_price_basis
    # if any descripancies found - we will receive notification in #temp_invoices slack channel and decide is it expected or not
    validation_dict = utils.validate_price_basis(transaction_results, period_start, period_end, invoice_bucket, chain_name)

                                        ### PROCESS INVOICES ###
    invoice_results = transform_claims.build_default_invoice_data(transaction_results, chain_class, period_start, period_end)
    revenue_share_usd = transaction_final_df['Revenue Share'].sum().quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    invoice_results_final = _update_invoice_results(invoice_results, revenue_share_usd)
    revenue_share_per_ncpdp = transaction_final_df.groupby('NCPDP')['Revenue Share'].sum().sort_values(ascending=False).reset_index()
    total_row = pd.DataFrame([['Grand Total', revenue_share_per_ncpdp['Revenue Share'].sum()]], columns=revenue_share_per_ncpdp.columns)
    revenue_share_per_ncpdp = pd.concat([revenue_share_per_ncpdp, total_row], ignore_index=True)

    albertsons_specific_invoice_dict = {
        "Chain specific label": "TOTALS:",
        'Total Administration Fee': invoice_results['grand total before share'].iloc[0],
        'Administration Fee Revenue Share': revenue_share_usd,
        }
    invoices_grouped_df = _select_invoice_columns_and_apply_formatting(invoice_results)

                                        ### DRAW INVOICE ###
    file_bytes, send_file_name = draw_invoice.create_invoice(invoices_grouped_df, chain_name.capitalize(), chain_class, albertsons_specific_invoice_dict)

                                        ### VALIDATE RESULTS ###
    sources_claims.test_invoice_results_vs_transaction_final(invoice_results_final, transaction_results)

                                        ### SEND FILES ###
    assert file_bytes != None, "No data for invoice file creation"

    if validation_dict['dataframe'].empty == False:
        utils.send_text_message_to_slack(kwargs.get('temp_slack_channel'), slack_bot_token, validation_dict['message'])

    iu.send_file(invoice_results_final, chain_name.upper(), send_file_name, file_bytes, slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)    

    _create_and_send_excel_utilization(transaction_final_df, revenue_share_per_ncpdp, transaction_file_name, slack_channel,
                            slack_bot_token, invoice_bucket, chain=chain_name.upper(), hsdk_env=hsdk_env, **kwargs)

def _create_and_send_excel_utilization(transactions_final_df, revenue_share_per_ncpdp, file_name, slack_channel,
                         slack_bot_token, invoice_bucket, chain, **kwargs):

    if not transactions_final_df.empty:

        file_bytes = _create_excel_utilization_file(transactions_final_df, revenue_share_per_ncpdp)
        send_file_name = file_name

        if file_bytes != None:
            iu.send_file([transactions_final_df, revenue_share_per_ncpdp], chain, send_file_name, file_bytes, slack_channel,
                      slack_bot_token, invoice_bucket, **kwargs)
        else:
            log.info("no data for file" + file_name)
    else:
        log.info("no data for file" + file_name)

def _create_excel_utilization_file(transactions_final_df, revenue_share_per_ncpdp):

    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    sheet_df = [('Claim Detailed Data', transactions_final_df),
                ('Revenue Share per NCPDP', revenue_share_per_ncpdp)]
    transactions_final_df.to_excel(writer, sheet_name='Claim Detailed Data',
                  startrow=1, header=False, index=False)
    revenue_share_per_ncpdp.to_excel(writer, sheet_name='Revenue Share per NCPDP',
                  startrow=1, header=False, index=False)

    workbook = writer.book
    for sheet_name, data in sheet_df:
        worksheet = writer.sheets[sheet_name]

        # write header in non-default format
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': False,
            'border': 0})

        for col_num, value in enumerate(data.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # size excel columns to match the data
        for i, col in enumerate(data.columns):
            column_len = data[col].astype(str).str.len().max()
            column_len = max(column_len, len(col)) + 2
            worksheet.set_column(i, i, column_len)

    writer.close()
    return output

def _preprocess_claims_df(raw_claims_df, period_start, period_end):
    claims_df = sources_claims.preprocess_claims(raw_claims_df, partners=True)
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, chain_class.BIN_NUMBERS)                           # filter dataframe by bin_number and authorization_number
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= chain_class.CONTRACT_START_DATE]                # go live for Albertsons contract 2025-04-01
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)

    return claims_df

def _select_fields_for_exporting(transactions_df):
    transactions_df['BIN+PCN+GROUP'] = transactions_df['bin_number'].astype('object').fillna('') + '+' + transactions_df['process_control_number'].fillna('') + '+' + transactions_df['group_id'].fillna('')
    transactions_df['Claim Authorization Number'] = transactions_df['authorization_number'].fillna('')
    transactions_df['product_id'] = transactions_df['product_id'].astype('string').str.zfill(11)
    transactions_df['NDC'] = transactions_df['product_id'].fillna('')
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Fill date'] = transactions_df['valid_from'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['claim_date_of_service'] = transactions_df['claim_date_of_service'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    transactions_df['Reversal date'] = np.where(transactions_df['reversal indicator'] != 'B2', '', transactions_df['Reversal date']) # set empty string instead of 2050-01-01 for fills
    transactions_df['NADAC Indicator'] = np.where(transactions_df['price_basis'] == 'NADAC', 'Y', 'N')
    transactions_df['Revenue Share'] = transactions_df.apply(_calculate_revenue_share, axis=1)
    transactions_df['Revenue Share'] = transactions_df['Revenue Share'] * transactions_df['fill_reversal_indicator']
    transactions_df = transactions_df.rename(columns=chain_class.COLUMN_RENAME_DICT) # rename columns 
    transactions_df = transactions_df[chain_class.REQUESTED_COLUMNS]

    return transactions_df

def _select_invoice_columns_and_apply_formatting(df):
    df = df[['period_start', 'period_end', 'invoice_date', 'due_date', 'net', 'invoice_number', 
                'drug_type', 'basis_of_reimbursement_source', 'grand total', 'claims count', 'total paid claims',
                'total remunerative paid claims', 'ingredient_cost_paid_resp', 'total_paid_response']].rename(columns={'ingredient_cost_paid_resp':'total ingredient cost paid', 'total_paid_response':'total administration fee owed'})
    df[['total paid claims','total remunerative paid claims']] = df[['total paid claims','total remunerative paid claims']].astype('int')
    df = utils.apply_formatting(df, columns=['total administration fee owed', 'total ingredient cost paid', 'grand total'], formatting='{:,.2f}')
    return df

def _update_invoice_results(invoice_results, revenue_share_usd):
    invoice_results['grand total before share'] = invoice_results['grand total']
    invoice_results['grand total'] = invoice_results['grand total'] - revenue_share_usd
    return invoice_results

def _calculate_revenue_share(row):
    admin_fee_floor = Decimal(chain_class.ADMIN_FEE_SHARE_FLOR)
    revenue_share_percent = Decimal(chain_class.REVENUE_SHARE_OVER_MARGIN_PERCENT)
    paid = abs(Decimal(row['total_paid_response']))
    if paid > admin_fee_floor: # if Admin Fee collected is more than $5
        share = revenue_share_percent * (paid - admin_fee_floor) # share 50% of the profit above $5 with Albertsons
        return share.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) # round 0.115 to 0.12
    else:
        return Decimal('0.00') # if Admin Fee is $5 or lower - all margin goes to the Hippo = nothig to share with Albertsons
