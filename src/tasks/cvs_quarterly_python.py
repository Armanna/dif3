#!/usr/bin/env python3
import pandas as pd
import io
from datetime import datetime
from decimal import Decimal

from transform import utils
from sources import pharmacies
from transform import claims as transform_claims
from tasks import download_other_sources
from hippo.sources.s3 import S3Downloader

from transform.cvs import cvs_claims
from . import invoice_utils as iu
from hippo import logger

log = logger.getLogger('Processing CVS') 

CHAIN = "CVS"

chain_name = 'cvs'
chain_class = pharmacies.CVS

def run(period, invoice_bucket, slack_channel, temp_slack_channel, slack_bot_token, manual_date_string, hsdk_env, **kwargs):
    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")
    log.info(f"Running {period}ly report.")
    log.info(f"Running date is {manual_date_string}.")
    temp_main_prefix = f"temp/sources/cvs/{period}/"
    temp_chain_specific_prefix = f"temp/sources/specific/cvs/{period}/"
    temp_claims_prefix = f"temp/claims/cvs/{period}"


    period_start, period_end = utils.process_invoice_dates(period_flag=period, manual_date_str=manual_date_string)

    log.info(f"Downloading the data for the period: {period_start} - {period_end}.")
    claims_df = S3Downloader(invoice_bucket, temp_claims_prefix, 'claims.parquet').pull_parquet()[0]
    log.info(f"Processing default source dictionary.")
    source_dict = download_other_sources.load_parquet_files_to_dict(invoice_bucket, temp_main_prefix)
    log.info(f"Processing CVS specific source dictionary.")
    cvs_source_dict = download_other_sources.load_parquet_files_to_dict(invoice_bucket, temp_chain_specific_prefix)
    log.info(f"Joining costs.")
    # join specific CVS sources: excluded_ndcs, mac/awp/wac costs, tpdt_specialty
    claims_df = cvs_claims.join_cvs_quarterly_dataframes(claims_df, cvs_source_dict)
    
    # process total quarterly transactions for cvs without webmd, cvs with webmd and cvs tpdt
    transaction_raw_df = transform_claims.build_default_transactions_dataset(claims_df, source_dict, chain_name, chain_class, period_start, period_end, slack_bot_token=slack_bot_token, temp_slack_channel=temp_slack_channel)
    transaction_raw_df = add_cvs_specific_columns(transaction_raw_df)

                                                ### VALIDATE PRICE BASIS ###
    # need this validation in order to ensure that we don't blindly reconcile agains reconciliation_price_basis
    # if any descripancies found - we will receive notification in #temp_invoices slack channel and decide is it expected or not
    validation_dict = utils.validate_price_basis(transaction_raw_df, period_start, period_end, invoice_bucket, chain_name)
    if validation_dict['dataframe'].empty == False:
        utils.send_text_message_to_slack(temp_slack_channel, slack_bot_token, validation_dict['message'])

    for column in ['reconciliation_price_basis','bin_number']:
        if transaction_raw_df[column].dtype.name == 'category':
            transaction_raw_df[column] = transaction_raw_df[column].cat.remove_unused_categories()
            transaction_raw_df[column] = transaction_raw_df[column].astype('object')

    summary_file_names_mapping = {}
    transactions_file_names_mapping = {}
    for line_of_business in transaction_raw_df['line_of_business'].unique():
        df = transaction_raw_df[transaction_raw_df['line_of_business'] == line_of_business]
        if not df.empty:

            transaction_file_name = f"{period_start.strftime('%Y-%m-%d')}_Hippo_CVS_{period}ly_Transactions_{line_of_business}_{invoice_date}.csv"
            summary_file_name = f"{period_start.strftime('%Y-%m-%d')}_Hippo_CVS_{period}ly_Summary_{line_of_business}_{invoice_date}.xlsx"

            transactions_df = select_partner_and_fields_for_exporting(df)
            summary_df = transform_cvs_quarterly_summary(df)

            summary_file_names_mapping[summary_file_name] = summary_df
            transactions_file_names_mapping[transaction_file_name] = transactions_df

    for filename, quarterly_file in summary_file_names_mapping.items():
        iu.create_and_send_file("summary", quarterly_file, filename, slack_channel,
                        slack_bot_token, invoice_bucket, chain=CHAIN, hsdk_env=hsdk_env, **kwargs)

    for filename, quarterly_file in transactions_file_names_mapping.items():
        if not quarterly_file.empty: # save as .csv in s3 bucket and text to #ivoices channel in slack
            file_bytes = io.BytesIO()
            quarterly_file.to_csv(file_bytes, index=False, encoding='utf-8')
            iu.send_file(quarterly_file, "CVS", filename, file_bytes,
                        slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs)

def select_partner_and_fields_for_exporting(transactions_df):
    transactions_df = transactions_df.rename(columns={'bin_number':'unique identifier','prescription_reference_number':'rx#','claim_date_of_service':'service date',
        'valid_from':'fill date','fill_number':'refill number','drug_name':'drug name','quantity_dispensed':'total quantity dispensed','days_supply':'total days supply',
        'usual_and_customary_charge': 'dispensing pharmacy u&c price', 'percentage_sales_tax_amount_paid': 'taxes paid', 'total_paid_response': 'administration fee',
        'patient_pay_resp': 'patient amount', 'full_cost': 'total cost', 'ingredient_cost_paid_resp': 'ingredient cost paid', 'dispensing_fee_paid_resp': 'dispensing fee paid',
        'actual effective rate':'effective rate', 'dfer_target_rate':'dispensing fee target rate', 'reconciliation_price_basis':'basis of reimbursement', 'drug_type':'generic indicator (multi-source indicator)', 'contracted_cost': 'contracted cost'})
    transactions_df = transactions_df[['bin+pcn+group', 'unique identifier', 'npi', 'claim authorization number', 'rx#', 'service date', 'fill date',
        'reversal date', 'refill number', 'ndc', 'drug name', 'generic indicator (multi-source indicator)', 'excluded claim indicator', 'exclusion reason', 'Reversal indicator', 'basis of reimbursement', 
        'total quantity dispensed', 'total days supply', 'dispensing pharmacy u&c price', 'taxes paid', 'administration fee', 'patient amount', 'total cost', 'ingredient cost paid', 'awp', 'mac', 'wac', 'contracted cost', 
        'effective rate', 'target effective rate', 'ic dollar variance', 'dispensing fee paid', 'dispensing fee target rate', 'df dollar variance',]]
    transactions_df[['fill date', 'service date']] = transactions_df[['fill date', 'service date']].applymap(lambda x: x.strftime('%Y-%m-%d'))
    return transactions_df

def transform_cvs_quarterly_summary(transactions_df):

    summary_df = transactions_df.groupby(['Contract Category', 'line_of_business', 'reconciliation_price_basis', 'target_rate', 'dfer_target_rate', 'admin_fee_rate'], as_index=False
        ).agg({
            'quantity_dispensed': 'sum',
            'fill_reversal_indicator': 'sum',
            'contracted_cost': 'sum',
            'ingredient_cost_paid_resp': 'sum',
            'dispensing_fee_paid_resp': 'sum',
            'total_paid_response': 'sum',
            'full_cost': 'sum',
        }).rename(columns={
            'fill_reversal_indicator':'Rx Volume',
            'quantity_dispensed':'Quantity Dispensed',
            'full_cost':'Brand WAC Price',
            'ingredient_cost_paid_resp': 'Adjudicated Ingredient Cost',
            'dispensing_fee_paid_resp': 'Adjudicated Dispense Fees',
            'contracted_cost': 'Contract Ingredient Cost',
            'dfer_target_rate': 'Contract Dispense Fees/Rx',
            'total_paid_response': 'Adjudicated Admin Fees',
            'admin_fee_rate': 'Contract Admin Fees/Rx'
            })
    summary_df['Adjudicated Dispense Fee/Rx'] = summary_df['Adjudicated Dispense Fees'] / summary_df['Rx Volume']
    summary_df['Contract Dispense Fees'] = summary_df['Rx Volume'] * summary_df['Contract Dispense Fees/Rx']
    summary_df['Cost Basis Performance'] = summary_df['Adjudicated Ingredient Cost'] - summary_df['Contract Ingredient Cost']
    summary_df['Dispense Fee Performance'] = summary_df['Adjudicated Dispense Fees'] - summary_df['Contract Dispense Fees']
    summary_df['Total Contractual Performance'] = summary_df['Cost Basis Performance'] + summary_df['Dispense Fee Performance']

    summary_df['Adjudicated Admin Fee/Rx'] = summary_df['Adjudicated Admin Fees'] / summary_df['Rx Volume']
    summary_df['Contract Admin Fees'] = summary_df['Rx Volume'] * summary_df['Contract Admin Fees/Rx']
    summary_df['Admin Fee Performance'] = summary_df['Adjudicated Admin Fees'] - summary_df['Contract Admin Fees']
    summary_df['Adjudicated Admin Fees'] = summary_df['Adjudicated Admin Fees']

    total_row = pd.DataFrame({
        'Contract Category': ['Total'],
        'line_of_business': [''],
        'reconciliation_price_basis': [''],
        'target_rate': [''],
        'Quantity Dispensed': [summary_df['Quantity Dispensed'].sum()],
        'Rx Volume': [summary_df['Rx Volume'].sum()],
        'Brand WAC Price': [summary_df['Brand WAC Price'].sum()],
        'Adjudicated Ingredient Cost': [summary_df['Adjudicated Ingredient Cost'].sum()],
        'Adjudicated Dispense Fees': [summary_df['Adjudicated Dispense Fees'].sum()],
        'Adjudicated Dispense Fee/Rx': [summary_df['Adjudicated Dispense Fees'].sum() / summary_df['Rx Volume'].sum()],
        'Contract Ingredient Cost': [summary_df['Contract Ingredient Cost'].sum()],
        'Contract Dispense Fees': [summary_df['Contract Dispense Fees'].sum()],
        'Contract Dispense Fees/Rx': [(summary_df['Rx Volume'] * summary_df['Contract Dispense Fees/Rx']).sum() / summary_df['Contract Dispense Fees/Rx'].sum()],
        'Cost Basis Performance': [summary_df['Cost Basis Performance'].sum()],
        'Dispense Fee Performance': [summary_df['Dispense Fee Performance'].sum()],
        'Total Contractual Performance': [summary_df['Total Contractual Performance'].sum()],
        'Adjudicated Admin Fees': [summary_df['Adjudicated Admin Fees'].sum()],
        'Adjudicated Admin Fee/Rx': [summary_df['Adjudicated Admin Fee/Rx'].mean()],
        'Contract Admin Fees/Rx': [summary_df['Contract Admin Fees'].sum() / summary_df['Rx Volume'].sum()],
        'Contract Admin Fees': [summary_df['Contract Admin Fees'].sum()],
        'Admin Fee Performance': [summary_df['Admin Fee Performance'].sum()],
    })

    summary_df = pd.concat([summary_df, total_row], ignore_index=True)
    
    for column in ['Adjudicated Admin Fee/Rx', 'Adjudicated Dispense Fee/Rx', 'Contract Dispense Fees/Rx']:
        summary_df[column] = summary_df[column].apply(lambda x: Decimal(str(x)).quantize(Decimal('.0001')))

    return summary_df[[
            'Contract Category', 
            'line_of_business',
            'reconciliation_price_basis',
            'target_rate',
            'Quantity Dispensed', 
            'Rx Volume', 
            'Brand WAC Price', 
            'Adjudicated Ingredient Cost', 
            'Adjudicated Dispense Fees', 
            'Adjudicated Dispense Fee/Rx', 
            'Contract Ingredient Cost', 
            'Contract Dispense Fees', 
            'Contract Dispense Fees/Rx', 
            'Cost Basis Performance', 
            'Dispense Fee Performance', 
            'Total Contractual Performance', 
            'Adjudicated Admin Fees', 
            'Adjudicated Admin Fee/Rx', 
            'Contract Admin Fees', 
            'Contract Admin Fees/Rx',
            'Admin Fee Performance',
        ]]

def add_cvs_specific_columns(df):
    df['line_of_business'] = chain_class.assign_line_of_business(df[['bin_number', 'network_reimbursement_id']])
    df['Contract Category'] = df['drug_type'].str.capitalize() + ' ' + df['days_supply_group'] + ' ' + 'DS'
    df['bin+pcn+group'] = df['bin_number'].astype('object').fillna('') + '+' + df['process_control_number'].fillna('') + '+' + df['group_id'].fillna('')
    df['claim authorization number'] = df['authorization_number'].fillna('')
    df['product_id'] = df['product_id'].astype('str').str.zfill(11)
    df['ndc'] = df['product_id'].astype('string').fillna('')
    df['reversal date'] = df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
    df['Reversal indicator'] = df['transaction_code']
    df['excluded claim indicator'] = 'N'
    df['exclusion reason'] = ''
    return df
