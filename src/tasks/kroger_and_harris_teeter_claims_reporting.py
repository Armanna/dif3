#!/usr/bin/env python3

import pandas as pd
import io
from datetime import datetime as dt
from decimal import Decimal

from sources import claims as sources_claims
from transform import claims as transform_claims
from transform import utils
from hippo.sources.download_sources import SourceDownloader, HistoricalData, MediSpan, HistoricalDataPbmHippo
from hippo.sources import claims_downloader
from hippo.sources.claims import FillsAndReversals, Chains, FillStatus
from sources import pharmacies
from transform.kroger_and_harris_teeter import transactions
from hippo import logger

from . import invoice_utils as iu


TARGET_GER = Decimal(pharmacies.KrogerAndHarrisTeeter.TARGET_GER) * Decimal('100')
TARGET_BER = Decimal(pharmacies.KrogerAndHarrisTeeter.TARGET_BER) * Decimal('100')

def run(invoice_bucket, slack_channel, slack_bot_token, hsdk_env, chain_name, period, **kwargs):

    log = logger.getLogger(f'Processing {chain_name} {period}ly')

    period_start, period_end = utils.process_invoice_dates(period_flag=period)
    # manually handle exclusion for Q2 2024 because we started processing Kroger claims within direct deal at 15th of May so within Q2 2024 we need to process 2024-05-15 - 2024-06-30
    period_start = pd.to_datetime('2024-05-15') if period_start < pd.to_datetime('2024-05-15') else period_start


    if chain_name == 'kroger':
        chain_enum = Chains.KROGER
    elif chain_name == 'harris_teeter':
        chain_enum = Chains.HARRIS_TEETER
    
    # downloads and process claims
    claims_src = claims_downloader.ClaimsDownloader(period_start = period_start, period_end = period_end, fills_and_reversals = FillsAndReversals.FILLS_AND_REVESALS, fill_status=FillStatus.FILLED, chains=chain_enum, print_sql_flag=True)
    raw_claims_df = claims_src.pull_data()
    claims_df = preprocess_claims_df(raw_claims_df, period_start, period_end) # apply all necessary filters and conditions for specific chain

    # downloads source files
    src = SourceDownloader(tables=[HistoricalData.PHARMACY_S3,HistoricalData.NDC_COST_V2_S3, HistoricalData.NDC_V2_S3,
                                   MediSpan.NAME_S3, MediSpan.NDC_S3, HistoricalDataPbmHippo.KROGER_MAC_CS_2_HISTORY,
                                   HistoricalData.DRUG_NDCS_WITH_RXNORM, HistoricalDataPbmHippo.KROGER_MAC_CS_3_4_5_HISTORY], get_modified_dfs=True)
    source_dict = src.generate_sources_dictionary()

                                            ### PROCESS TRANSACTIONS ###
    transaction_raw = transactions.transform_transactions_data(claims_df, source_dict, period_start, period_end)
    transaction_results = _select_fields_for_exporting(transaction_raw)
    transaction_file_name = f"Hippo_{chain_name.upper()}_{period.capitalize()}ly_Transactions_" + dt.today().strftime("%m%d%Y") + ".xlsx"

    log.info('processing: calculate Benchmark Compliance')
    benchmark_compliance_df = generate_benchmark_compliance_per_date_of_service(transaction_raw)

    # Exclude mac columns for reports before 2025
    columns_to_save = [
        'generic indicator (multi-source indicator)', 'Total Number of Claims', 'Total Aggregate AWP',
        'Total Aggregate Ingredient Costs', 'AWP Discount Rate Compliance', 'Total Aggregate NADAC' ,
        'NADAC compliance']
    if period_start >= pd.to_datetime('2025-01-01'):
        columns_to_save += ['Total Aggregate MAC', 'MAC compliance']

    benchmark_bytes = save_benchmark_compliance_to_excel(benchmark_compliance_df[columns_to_save], period_start, period_end)

    to_send_dict = {
        'benchmark_report': {
            'df': benchmark_compliance_df,
            'bytes': benchmark_bytes,
            'file_name': f"Hippo_{chain_name}_BENCHMARK_COMPLIANCE_" + dt.today().strftime("%m%d%Y") + ".xlsx"
        },
    }

    for name, bytes in to_send_dict.items():
        if bytes != None:
            iu.send_file(bytes['df'], chain_name, bytes['file_name'], bytes['bytes'], slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs) 
        else:
            log.info("no data for file" + name)

    iu.create_and_send_file("transactions", transaction_results, transaction_file_name, slack_channel,
                            slack_bot_token, invoice_bucket, chain=chain_name, hsdk_env=hsdk_env, **kwargs)

def preprocess_claims_df(raw_claims_df, period_start, period_end):
    chain = pharmacies.KrogerAndHarrisTeeter
    claims_df = sources_claims.preprocess_claims(raw_claims_df)
    claims_df = transform_claims.filter_by_binnumber_authnumber(claims_df, chain.BIN_NUMBERS, chain.CHAIN_CODES)        # filter dataframe by chan_code, bin_number and authorization_number
    claims_df = sources_claims.filter_reversals(claims_df, period_start, period_end)
    claims_df = claims_df[claims_df['claim_date_of_service'].dt.floor('D') >= chain.CONTRACT_START_DATE]                # go live for Kroger/HT contract 2024-05-15
    return claims_df

def _select_fields_for_exporting(transactions_df):
    transactions_df = transactions_df[
        ['Prescription number', 'Refill number', 'NCPDP', 'Date of Service', 'Claim number', 'NDC', 'MONY code', "Days' supply", 'Total quantity dispensed', 'Paid Ingredient Cost', 'Total Pharmacy reimbursement', 'Patient Paid Amount', 'Paid Dispense Fee',
        'AWP', 'NADAC', 'MAC', 'Basis of reimbursement determination', 'Claim reference number', 'Unique Identifier', 'Administration Fee', 'Tax paid', 'COB indicator', 'Usual and Customary Charge (U&C)', 'DEA Class Code',
        'Excluded Claim Indicator', 'Exclusion reason', 'Fill date', 'NPI', 'claim_type', 'generic indicator (multi-source indicator)', 'Reversal date', 'reversal indicator']]
    return transactions_df

def generate_benchmark_compliance_per_date_of_service(transaction_df):
    # Filter out reversals, excluded claims, and U&C claims
    transaction_df = transaction_df[(transaction_df['reversal indicator'] != 'B2') & 
                                    (transaction_df['Excluded Claim Indicator'] == 'N') & 
                                    (transaction_df['Basis of reimbursement determination'] != 'UNC')]
    
    transaction_df['Basis of reimbursement determination'] = transaction_df['Basis of reimbursement determination'].astype(str)

    grouped_df = transaction_df.groupby(['generic indicator (multi-source indicator)', 'Basis of reimbursement determination'], as_index=False).agg({
        'Claim number': 'count',
        'Paid Ingredient Cost': 'sum', 
        'AWP': 'sum', 
        'NADAC': 'sum',
        'MAC': 'sum'
    })

    # AWP Discount Rate Compliance calculation
    grouped_awp_df = grouped_df[grouped_df['Basis of reimbursement determination'] == 'AWP']
    grouped_awp_df['AWP Discount Rate Compliance'] = (grouped_awp_df['AWP'] - grouped_awp_df['Paid Ingredient Cost']) / grouped_awp_df['AWP']

    # NADAC compliance calculation
    grouped_nadac_df = grouped_df[grouped_df['Basis of reimbursement determination'] == 'NADAC']
    grouped_nadac_df['NADAC compliance'] = (grouped_nadac_df['NADAC'] - grouped_nadac_df['Paid Ingredient Cost']) / grouped_nadac_df['NADAC']

    # MAC compliance calculation
    grouped_mac_df = grouped_df[grouped_df['Basis of reimbursement determination'] == 'MAC']
    grouped_mac_df['MAC compliance'] = (grouped_mac_df['MAC'] - grouped_mac_df['Paid Ingredient Cost']) / grouped_mac_df['MAC']

    # Final aggregation
    final_grouped_df = grouped_df.groupby(['generic indicator (multi-source indicator)'], as_index=False).agg({
        'Claim number': 'sum',
        'Paid Ingredient Cost': 'sum'
    }).rename(columns={'Claim number': 'Total Number of Claims', 'Paid Ingredient Cost': 'Total Aggregate Ingredient Costs'})

    final_grouped_df = pd.merge(final_grouped_df, 
                                grouped_awp_df[['generic indicator (multi-source indicator)', 'AWP', 'AWP Discount Rate Compliance']],
                                how='left', 
                                on='generic indicator (multi-source indicator)')
    final_grouped_df = final_grouped_df.rename(columns={'AWP': 'Total Aggregate AWP'})

    final_grouped_df = pd.merge(final_grouped_df, 
                                grouped_nadac_df[['generic indicator (multi-source indicator)', 'NADAC', 'NADAC compliance']], 
                                how='left', 
                                on='generic indicator (multi-source indicator)')
    final_grouped_df = final_grouped_df.rename(columns={'NADAC': 'Total Aggregate NADAC'})


    final_grouped_df = pd.merge(final_grouped_df,
                                grouped_mac_df[
                                    ['generic indicator (multi-source indicator)', 'MAC', 'MAC compliance']],
                                how='left',
                                on='generic indicator (multi-source indicator)')
    final_grouped_df = final_grouped_df.rename(columns={'MAC': 'Total Aggregate MAC'})

    return final_grouped_df

def save_benchmark_compliance_to_excel(dataframe, period_start, period_end):
    
    # Convert period_start and period_end to strings
    period_start_str = period_start.strftime('%Y-%m-%d')
    period_end_str = period_end.strftime('%Y-%m-%d')
    
    # Create an in-memory output file for the new Excel file
    output = io.BytesIO()

    # Create a Pandas Excel writer using XlsxWriter as the engine
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Get the workbook and add a worksheet
        workbook = writer.book
        worksheet = workbook.add_worksheet('Sheet1')
        
        # Write the reporting period information
        worksheet.write('A1', f'Reporting Period: {period_start_str} - {period_end_str}')
        
        # Define formatting
        accountant_format = workbook.add_format({'num_format': '$#,##0.00'})
        percentage_format = workbook.add_format({'num_format': '0.00%'})
        
        # Write the column headers
        for col_num, value in enumerate(dataframe.columns.values):
            worksheet.write(2, col_num, value)
        
        # Write the data and apply formatting
        for row_num, row in enumerate(dataframe.values, start=3):
            for col_num, value in enumerate(row):
                if pd.isna(value):
                    worksheet.write(row_num, col_num, '')  # Write empty string for NaN
                else:
                    if dataframe.columns[col_num] in ['Total Aggregate AWP', 'Total Aggregate Ingredient Costs', 'Total Aggregate NADAC', 'Total Aggregate MAC']:
                        worksheet.write(row_num, col_num, value, accountant_format)
                    elif dataframe.columns[col_num] in ['AWP Discount Rate Compliance', 'NADAC compliance', 'MAC compliance']:
                        worksheet.write(row_num, col_num, value, percentage_format)
                    else:
                        worksheet.write(row_num, col_num, value)
        
        # Adjust column width
        for i, col in enumerate(dataframe.columns):
            max_len = max(dataframe[col].astype(str).apply(len).max(), len(col)) + 2
            worksheet.set_column(i, i, max_len)
    
    # Seek to the beginning of the stream
    output.seek(0)
    
    return output
