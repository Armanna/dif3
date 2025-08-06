#!/usr/bin/env python3

import pandas as pd
import numpy as np
import io
from decimal import Decimal
from transform import utils
from datetime import datetime

from sources import redshift
from . import invoice_utils as iu
from hippo import logger

log = logger.getLogger('Rite Aid yearly')

TARGET_GER = Decimal('30') # current targeted Rite Aid GER
TARGET_BER = Decimal('17') # current targeted Rite Aid BER
PRICE_BASIS = ['non-NADAC', 'NADAC']

def run(invoice_bucket, period, slack_channel, slack_bot_token, hsdk_env, **kwargs):
    redshift_src = redshift.Redshift()
    today = datetime.today()
    invoice_date = today.strftime("%m%d%Y")
    period_start, period_end = utils.process_invoice_dates(period_flag=period)
    period_label = f"{period.capitalize()}ly"
    to_send_dict = {}
    log.info('run riteaid-summary.sql')
    transaction_raw = redshift_src.pull(
        "sources/RiteAid-summary.sql", params={"period_start": period_start, "period_end": period_end})
    transaction_raw = format_columns(transaction_raw, columns=['total quantity dispensed','ingredient cost paid','total paid response','dispensing fee paid','contracted_disp_fee','contracted_awp','average wholesale price (awp)','contracted_nadactr','dispensing pharmacy u&c price'])

    if period == 'month':
        log.info('processing: summary')
        ger_results = generate_ger(transaction_raw, period_start)
        ber_results = generate_ber(transaction_raw)
        awp_summary, nadac_summary = generate_summary(
            transaction_raw,
            ger_results['Actual GER'].iloc[0],
            ber_results['Actual BER'].iloc[0],
        )
        date_period = f"Period: {period_start:%Y-%m-%d} - {period_end:%Y-%m-%d}"
        invoice_date = invoice_date
        raw_summaries = {
            'AWP': awp_summary,
            'NADAC': nadac_summary,
        }
        for label, summary_dict in raw_summaries.items():
            df_all = pd.concat(summary_dict.values(), axis=0, ignore_index=True)
            bytes_ = write_dataframes_to_excel(summary_dict, date_period, label)
            to_send_dict[f'{label.lower()}_summary_report'] = {
                'df': df_all,
                'bytes': bytes_,
                'file_name': f"Hippo_RiteAid_{label}_{period_label}_Summary_{invoice_date}.xlsx",
            }

    if period in ('quarter', 'year'):
        for price_basis in PRICE_BASIS:
            log.info(f'processing: {period} {price_basis} utilization')
            if price_basis == 'NADAC':
                mask = (transaction_raw['fill_type'] == price_basis)
            else:
                mask = (transaction_raw['fill_type'] != 'NADAC')
            cols = ['rx#', 'ncpdp', 'unique identifier', 'fill date', 'ndc', 'pricing type', 'drug type indicator',
                 'total quantity dispensed', 'ingredient cost paid', 'total paid response',
                 'reversal indicator', 'total reimbursement to pharmacy', 'copay amount',
                 'dispensing fee paid', 'awp unit cost', 'nadac unit cost', 'dispensing pharmacy u&c price',
                 'claim authorization number', 'network id', 'pcn', 'excluded claim indicator', 'exclusion reason',  'administration fee']
            transaction_results = transaction_raw.loc[mask]
            log.info(f'processing: {period} {price_basis} summary info')
            summary_results = generate_effective_rate_summary(transaction_results, price_basis)
            summary_bytes = io.BytesIO()
            summary_results.to_csv(summary_bytes, index=False, encoding='utf-8')
            transaction_file_name = f"Hippo_RiteAid_{price_basis}_{period_label}_Utilization_{invoice_date}.xlsx"
            iu.create_and_send_file("transactions", transaction_results[cols], transaction_file_name, slack_channel,
                                    slack_bot_token, invoice_bucket, chain="Rite_aid", hsdk_env=hsdk_env, **kwargs)
            to_send_dict[f'{price_basis.lower()}_summary'] = {
                'df': summary_results,
                'bytes': summary_bytes,
                'file_name': f"Hippo_RiteAid_{price_basis}_{period_label}_Effective_Rate_Report_{invoice_date}.csv"
        }

    for name, bytes in to_send_dict.items():
        if bytes != None:
            iu.send_file(bytes['df'], 'Rite_aid', bytes['file_name'], bytes['bytes'], slack_channel, slack_bot_token, invoice_bucket, hsdk_env=hsdk_env, **kwargs) 
        else:
            log.info("no data for file" + name)


def ger_diff(row):
    if row['Actual GER'] < row['Targeted GER']:
        return f"Actual GER less than contracted by {row['Targeted GER'] - row['Actual GER']} %"
    elif row['Actual GER'] > row['Targeted GER']:
        return f"Actual GER exceed contracted by {row['Targeted GER'] - row['Actual GER']} %"
    else:
        return 'Actual GER equal to contracted'


def generate_ger(df, period_start):
    df = df[(df['excluded claim indicator'] == 'N') & (df['reversal indicator'] != 'B2') & (df['fill_type'] == 'AWP')  & (df['drug type indicator'] == 'G')]
    df_grouped = df.groupby([df['fill_type']], as_index=False) \
        .agg({'rx#':'count','contracted_awp':'sum','average wholesale price (awp)':'sum','ingredient cost paid':'sum','dispensing fee paid':'sum','contracted_disp_fee':'sum'}) \
        .rename(columns={'rx#':'Total claims'})
    df_grouped["Actual GER"] = ((1 - df_grouped['ingredient cost paid']/df_grouped['average wholesale price (awp)']) * Decimal('100')).apply(lambda x: x.quantize(Decimal('.01')))
    df_grouped["Targeted GER"] = TARGET_GER
    df_grouped['GER diff'] = df_grouped.apply(lambda row: ger_diff(row), axis=1)
    df_grouped['Dollar variance'] = (df_grouped['contracted_awp'] - df_grouped['ingredient cost paid']).apply(lambda x: x.quantize(Decimal('.0001')))
    df_grouped['Calendar year'] = period_start.year
    df_grouped = df_grouped[['Calendar year', 'Total claims', 'Actual GER', 'Targeted GER', 'GER diff', 'Dollar variance']]
    return df_grouped


def generate_ber(df):
    df = df[(df['reversal indicator'] != 'B2') & (df['fill_type'] == 'AWP') & (df['drug type indicator'] == 'B')]
    df_grouped = df.groupby([df['fill_type']], as_index=False) \
        .agg({'rx#':'count','contracted_awp':'sum','average wholesale price (awp)':'sum','ingredient cost paid':'sum','dispensing fee paid':'sum','contracted_disp_fee':'sum'}) \
        .rename(columns={'rx#':'Total claims'})
    df_grouped["Actual BER"] = ((1 - df_grouped['ingredient cost paid']/df_grouped['average wholesale price (awp)']) * Decimal('100')).apply(lambda x: x.quantize(Decimal('.01')))
    return df_grouped


def generate_summary(df, actual_ger_rate, actual_ber_rate):
    df = df[(df['reversal indicator'] != 'B2') & (df['basis_of_reimbursement_source'] != 'UNC')]
    awp_grouped_df = df[(df['fill_type'] == 'AWP')].groupby('drug type indicator', as_index=False) \
        .agg({'rx#':'count','contracted_awp':'sum','average wholesale price (awp)':'sum','ingredient cost paid':'sum'}) \
        .rename(columns={'rx#':'Total claims','contracted_awp':'Contracted AWP','ingredient cost paid':'Total Ingredient Cost Paid','average wholesale price (awp)':'Total AWP'})
    awp_grouped_df.loc[awp_grouped_df['drug type indicator'] == 'G', 'Contracted AWP Rate'] = f"{TARGET_GER}%"
    awp_grouped_df.loc[awp_grouped_df['drug type indicator'] == 'B', 'Contracted AWP Rate'] = f"{TARGET_BER}%"
    awp_grouped_df.loc[awp_grouped_df['drug type indicator'] == 'G', 'Actual GER'] = f'{actual_ger_rate}%'
    awp_grouped_df.loc[awp_grouped_df['drug type indicator'] == 'B', 'Actual GER'] = f'{actual_ber_rate}%'
    awp_grouped_df['AWP Variant Amount'] = (awp_grouped_df['Contracted AWP'] - awp_grouped_df['Total Ingredient Cost Paid']).apply(lambda x: x.quantize(Decimal('.01')))
    nadac_grouped_df = df[(df['fill_type'] == 'NADAC')].groupby('drug type indicator', as_index=False) \
        .agg({'rx#':'count','contracted_nadactr':'sum','ingredient cost paid':'sum'}) \
        .rename(columns={'rx#':'Total claims','contracted_nadactr':'Contracted NADACTR','ingredient cost paid':'Total Ingredient Cost Paid'})
    nadac_grouped_df['NADAC Variant Amount'] = (nadac_grouped_df['Contracted NADACTR'] - nadac_grouped_df['Total Ingredient Cost Paid']).apply(lambda x: x.quantize(Decimal('.01')))
    awp_grouped_fee_df = df[(df['fill_type'] == 'AWP')].groupby('drug type indicator', as_index=False) \
        .agg({'dispensing fee paid':'sum','contracted_disp_fee':'sum'}) \
        .rename(columns={'dispensing fee paid':'Actual Dispensing Fee Paid','contracted_disp_fee':'Contracted Dispensing Fee'})
    awp_grouped_fee_df['Dispense Fee Variant Amount'] = awp_grouped_fee_df['Actual Dispensing Fee Paid'] - awp_grouped_fee_df['Contracted Dispensing Fee']
    nadac_grouped_fee_df = df[(df['fill_type'] == 'NADAC')].groupby('drug type indicator', as_index=False) \
        .agg({'dispensing fee paid':'sum','contracted_disp_fee':'sum'}) \
        .rename(columns={'dispensing fee paid':'Actual Dispensing Fee Paid','contracted_disp_fee':'Contracted Dispensing Fee'})
    nadac_grouped_fee_df['Dispense Fee Variant Amount'] = nadac_grouped_fee_df['Actual Dispensing Fee Paid'] - nadac_grouped_fee_df['Contracted Dispensing Fee']
    avg_admin_fee_df = pd.DataFrame({'AFER': [df['total paid response'].mean()], 'drug type indicator': ''})
    awp_views = make_all_views(awp_grouped_df, awp_grouped_fee_df, avg_admin_fee_df, 'AWP')
    nadac_views = make_all_views(nadac_grouped_df, nadac_grouped_fee_df, avg_admin_fee_df, 'NADAC')
    return awp_views, nadac_views

def format_columns(df, columns):
    df = utils.cast_columns_to_decimal(df, column_names=columns, fillna_flag = True)
    df[['contracted_awp','average wholesale price (awp)','contracted_nadactr']] = df[['contracted_awp','average wholesale price (awp)','contracted_nadactr']].applymap(lambda x: x.normalize())
    return df


def pivot_and_label(df, indicator='drug type indicator'):
    df = (
        df
        .set_index(indicator)
        .transpose()
        .assign(**{
            'Total variant amt': lambda d: np.where(
                d.index.str.endswith('Variant Amount'),
                d.sum(axis=1),
                ''
            )
        })
    )
    for old, new in [('B','Brand'), ('G','Generic')]:
        if old in df.columns:
            df.rename(columns={old:new}, inplace=True)
        else:
            df[new] = 0.0
    return df[['Brand','Generic','Total variant amt']]


def make_all_views(grouped_df, fee_df, avg_fee_df, prefix):
    totals_df = compute_totals(grouped_df, fee_df)
    raw = {
        f"{prefix} Report": grouped_df,
        f"{prefix} Claims": fee_df,
        "Total": totals_df,
    }
    views = {
        name: pivot_and_label(df)
        for name, df in raw.items()
    }
    views["AFER"] = avg_fee_df.set_index("drug type indicator").T
    return views


def compute_totals(grouped_df, fee_df, indicator='drug type indicator'):
    df = pd.concat([grouped_df, fee_df], ignore_index=True)
    var_cols = df.filter(like='Variant Amount').columns
    total = (
        df
        .groupby(indicator, as_index=False)[var_cols]
        .sum()
        .assign(**{'Total Variant Amount': lambda d: d[var_cols].sum(axis=1)})
        [[indicator, 'Total Variant Amount']]
    )
    return total


def write_dataframes_to_excel(df_dict, dates, prefix):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter', options={'nan_inf_to_errors': True}) as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Summary")

        header_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        money_format = workbook.add_format({'num_format': '$#,##0.00', 'bold': False, 'border': 1, 'align': 'right'})
        border_format = workbook.add_format({'border': 1, 'align': 'right'})

        current_row = 0 # Need to track the row position for each dataframe
        worksheet.merge_range(current_row, 0, current_row, 3, f"{dates}", header_format)
        current_row += 1

        for name, df in df_dict.items():
            df.reset_index(inplace=True)
            if name in ['AWP Report', 'NADAC Report', 'AWP Claims', 'NADAC Claims']:
                if name in [ 'AWP Claims', 'NADAC Claims']:
                    worksheet.merge_range(current_row, 0, current_row, df.shape[1] - 1, "DISPENSING FEE REPORT", header_format)
                    current_row += 2
                worksheet.merge_range(current_row, 0, current_row, df.shape[1] - 1, f"{name}", header_format)
                current_row += 1
                for c, col in enumerate(df.columns):
                    if col != 'index':
                        worksheet.write(current_row, c, col, header_format)         
                current_row += 1

            for r, row in enumerate(df.itertuples(index=False)):
                for c, value in enumerate(row):
                    if df.columns[c] in ['Brand', 'Generic']:
                        if df['index'][r] in ['Contracted NADACTR', 'Total Ingredient Cost Paid', 'Contracted AWP', 'Total AWP', 'AFER', 'NADAC Variant Amount', 'AWP Variant Amount', 'Total Variant Amount', 'Actual Dispensing Fee Paid', 'Contracted Dispensing Fee', 'Dispense Fee Variant Amount']:
                            worksheet.write(current_row + r, c, value, money_format)
                        else:
                            worksheet.write(current_row + r, c, value, border_format)
                    elif df.columns[c] in ['Total variant amt']:
                        if df['index'][r].endswith('Variant Amount'):
                            worksheet.write(current_row + r, c, Decimal(value), money_format)
                        else:
                            worksheet.write(current_row + r, c, value, border_format)
                    elif df['index'][r] in ['Total claims', 'Contracted AWP Rate', 'Actual GER']:
                        worksheet.write(current_row + r, c, value, border_format)
                    elif df['index'][r] in ['Total Variant Amount']:
                        worksheet.write(current_row + r, c, value, header_format)
                    else:
                        worksheet.write(current_row + r, c, value, money_format)
            current_row += df.shape[0] + 1

        writer.close()
        return output


def effective_rate_diff(row):
    if row['Actual Effective Rate'] < row['Targeted Effective Rate']:
        return f"Actual Effective Rate less than contracted by {row['Targeted Effective Rate'] - row['Actual Effective Rate']} %"
    elif row['Actual Effective Rate'] > row['Targeted Effective Rate']:
        return f"Actual Effective Rate exceed contracted by {row['Targeted Effective Rate'] - row['Actual Effective Rate']} %"
    else:
        return 'Actual Effective Rate equal to contracted'


def generate_effective_rate_summary(df, price_basis):
    df = df[(df['excluded claim indicator'] == 'N') & (df['reversal indicator'] != 'B2')]
    df_grouped = df.groupby([df['drug type indicator']], as_index=False) \
        .agg({'rx#':'count','contracted ingredient cost':'sum','average wholesale price (awp)':'sum','ingredient cost paid':'sum','dispensing fee paid':'sum','contracted_disp_fee':'sum','ingredient cost':'sum'}) \
        .rename(columns={'rx#':'Total claims', 'drug type indicator':'Drug Type'})
    df_grouped["Actual Effective Rate"] = ((1 - df_grouped['ingredient cost paid']/df_grouped['ingredient cost']) * Decimal('100')).apply(lambda x: x.quantize(Decimal('.01')))
    df_grouped["Targeted Effective Rate"] = np.where(price_basis == "NADAC", 0, np.where(df_grouped["Drug Type"] == "G", TARGET_GER, TARGET_BER))
    df_grouped['Effective Rate diff'] = df_grouped.apply(lambda row: effective_rate_diff(row), axis=1)
    df_grouped['Dollar variance'] = (df_grouped['contracted ingredient cost'] - df_grouped['ingredient cost paid']).apply(lambda x: x.quantize(Decimal('.01')))
    df_grouped['Drug Type'] = df_grouped['Drug Type'].replace({'G': 'Generic', 'B': 'Brand'})
    df_grouped = df_grouped[['Drug Type', 'Total claims', 'Actual Effective Rate', 'Targeted Effective Rate', 'Effective Rate diff', 'Dollar variance']]
    return df_grouped
