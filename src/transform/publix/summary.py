import pandas as pd
import numpy as np
import decimal
import io

from decimal import Decimal, ROUND_HALF_DOWN, ROUND_HALF_UP
from transform import utils, claims, invoices
from sources import claims as sources_claims
from hippo import logger

log = logger.getLogger("publix marketplace summary")


def build_default_transactions_dataset(raw_claims_df, source_dict, chain_name, chain_class, period_start, period_end, **kwargs):
    raw_claims_df = sources_claims.join_default_source_dataframes(raw_claims_df, source_dict)

    raw_claims_df = claims.filter_by_binnumber_authnumber(raw_claims_df, chain_class.BIN_NUMBERS, chain_class.CHAIN_CODES) # filter dataframe by bin_number and authorization_number
    raw_claims_df = utils.cast_cents_to_dollars(raw_claims_df, column_names=['usual_and_customary_charge', 'percentage_sales_tax_amount_paid', 'total_paid_response', 'patient_pay_resp', 'ingredient_cost_paid_resp', 'dispensing_fee_paid_resp','awp', 'mac', 'wac', 'nadac', 'gpi_nadac'], add_fill_reversal_sign=False)
    raw_claims_df['drug_type'] = chain_class.add_brand_generic_indicator(raw_claims_df)
    raw_claims_df['claim_program_name'] = chain_class.add_program_flag(raw_claims_df)
    raw_claims_df['price_basis'] = claims.determine_basis_of_rembursment_abbreviation(raw_claims_df['basis_of_reimbursement_determination_resp'])
    raw_claims_df['reconciliation_price_basis'] = chain_class.define_reconciliation_price_basis(raw_claims_df)
    raw_claims_df['transaction_type'] = claims.define_transaction_type(raw_claims_df)

    chain_rates_df = source_dict['chain_rates'][source_dict['chain_rates']['chain_name'] == chain_name]
    # rename Controlled Drug C-II value to schedule_2 on column 'program_name'
    chain_rates_df['program_name'] = chain_rates_df['program_name'].replace({'Controlled Drug C-II': 'schedule_2', '': 'regular'})

    transactions_df = _join_chain_rates(raw_claims_df, chain_rates_df, column_to_merge='drug_type', **kwargs).rename(columns={'bin_number_x':'bin_number', 'bin_number_y':'bin_number_chain_rates','partner_y':'partner_chain_rates','partner_x':'partner_carholders'})

    transactions_df['fill_reversal_indicator'] = claims.add_fills_reversals_indicator(transactions_df, period_start, period_end)
    transactions_df['reversal indicator'] = claims.define_reversal_indicator(transactions_df, period_start, period_end, valid_from_column='valid_from')
    costs_df = claims.calculate_contracted_and_full_cost(transactions_df) # this function add two new columns: contracted_cost and full_cost
    transactions_df = transactions_df.join(costs_df)
    for col_name, pd_series in {'dispensing_fee_paid_resp': transactions_df['dispensing_fee_paid_resp'], 'ingredient_cost_paid_resp':transactions_df['ingredient_cost_paid_resp'], 'total_paid_response': -transactions_df['total_paid_response'], 'patient_pay_resp': transactions_df['patient_pay_resp'], 'contracted_cost': transactions_df['contracted_cost'], 'full_cost': transactions_df['full_cost'], 'percentage_sales_tax_amount_paid': transactions_df['percentage_sales_tax_amount_paid'], 'usual_and_customary_charge': transactions_df['usual_and_customary_charge']}.items():
        transactions_df[col_name] = claims.apply_sign_for_fills_and_reversals(transactions_df, pd_series)
    transactions_df = claims.set_precision_for_numeric_columns(transactions_df)
    df = claims.calculate_effective_rate_and_dollar_variance(transactions_df, price_basis_column='reconciliation_price_basis', **kwargs)
    transactions_df[['actual effective rate','target effective rate','effective rate variance','ic dollar variance','df dollar variance']] = df
    if hasattr(chain_class, 'set_exclusion_flags'): # if there are any exclusions for specific chain - run function; else - set default values
        transactions_df[['Excluded Claim Indicator', 'Exclusion reason']] = transactions_df.apply(
            chain_class.set_exclusion_flags, axis=1, result_type='expand'
        )
    else:
        transactions_df['Excluded Claim Indicator'] = "N"
        transactions_df['Exclusion reason'] = ""
    return transactions_df

def _join_chain_rates(sources_df, chain_rates_df, column_to_merge='drug_type', **kwargs):
    """
    This function properly joins chain_rates based on the claim_date_of_service and also transforms days_supply_from and days_supply_to into days_supply_group
    column which indicates what days_supply group each claim belongs to i.e. Any, 0-83, 84+, 0-89, 90+ etc. With this column it will be easier to group up
    the data for any reconciliation report.
    """

    chain_rates_df = chain_rates_df.copy()
    chain_rates_df[['days_supply_from', 'days_supply_to']] = chain_rates_df[['days_supply_from', 'days_supply_to']].astype('int')

    original_size = len(sources_df) # save the original number of rows

    # Separate transactions with price_basis == 'UNC' because we don't have UNC in chain_rates
    unc_transactions = sources_df[sources_df['reconciliation_price_basis'] == 'UNC']
    non_unc_transactions = sources_df[sources_df['reconciliation_price_basis'] != 'UNC']

    columns_to_update = ['target_rate', 'dfer_target_rate']
    unc_transactions[columns_to_update] = Decimal('0')

    # Drop unused categories in both DataFrames in order to avoid errors
    non_unc_transactions['bin_number'] = non_unc_transactions['bin_number'].cat.remove_unused_categories()
    non_unc_transactions['price_basis'] = non_unc_transactions['price_basis'].cat.remove_unused_categories()
    chain_rates_df['bin_number'] = chain_rates_df['bin_number'].cat.remove_unused_categories()

    try:
        sources_claims.check_value_combination_before_join_chain_rates(non_unc_transactions, chain_rates_df, column_to_merge) # run the test to ensure all distinct values from source_dict merging columns presented in chain_rates_df as well
        merged_df = pd.merge(
            non_unc_transactions,
            chain_rates_df,
            how='left',
            left_on=['bin_number', column_to_merge, 'claim_program_name', 'reconciliation_price_basis'],
            right_on=['bin_number', 'drug_type', 'program_name', 'price_basis'],
            suffixes=('_source', '_chain_rates')
        )
    except ValueError as e:
        log.info(e)
        return None

    # delete incorrect combinations
    filtered_df = merged_df[
        (merged_df['claim_date_of_service'] >= merged_df['valid_from_chain_rates']) &
        (merged_df['claim_date_of_service'] <= merged_df['valid_to_chain_rates']) &
        (merged_df['days_supply'] >= merged_df['days_supply_from']) &
        (merged_df['days_supply'] <= merged_df['days_supply_to'])
    ]

    def _determine_days_supply_group(row):
        if row['days_supply_from'] == 0 and row['days_supply_to'] == 99999:
            return 'Any'
        elif row['days_supply_from'] == 0:
            return f"{row['days_supply_from']} - {row['days_supply_to']}"
        else:
            return f"{row['days_supply_from']} +"
    filtered_df['days_supply_group'] = filtered_df.apply(_determine_days_supply_group, axis=1)

    preferred_matches = filtered_df[filtered_df['partner_source'] == filtered_df['partner_chain_rates']]
    fallback_matches = filtered_df[filtered_df['partner_chain_rates'] == ""]
    rest_matches = filtered_df[(filtered_df['partner_source'] != filtered_df['partner_chain_rates']) & (filtered_df['partner_chain_rates'] != "")]

    # this logic helps to process unique combinations chain+partner i.e. like we have with CVS (CVS + webmd or CVS + famulus); exact match by partner is preferred option if we have duplicates
    combined_matches = pd.concat([preferred_matches, fallback_matches, rest_matches]).drop_duplicates(
        subset=['authorization_number'],
        keep='first'
    )
    # Drop unnecessary columns and rename as needed
    combined_matches = combined_matches.drop(columns=[
        'valid_from_chain_rates', 'valid_to_chain_rates',
        'chain_name', 'days_supply_from', 'days_supply_to'
    ])
    # Rename columns to maintain consistency
    combined_matches = combined_matches.rename(columns={
        'partner_chain_rates': 'partner',
        'valid_from_source': 'valid_from',
        'valid_to_source': 'valid_to',
        'price_basis_source': 'price_basis',
        'state_abbreviation_source': 'state_abbreviation'
    })

    # Concatenate the UNC transactions back with the combined matches
    final_df = pd.concat([combined_matches, unc_transactions], ignore_index=True)

    final_size = len(final_df) # ensure that the size of the final DataFrame is the same as the original sources_df
    if final_size != original_size:
        log.info(f"Row count mismatch: original={original_size}, final={final_size}")

        # Find mismatched rows
        original_auth_numbers = set(sources_df['authorization_number'])
        final_auth_numbers = set(final_df['authorization_number'])

        # Rows missing from final_df
        missing_auth_numbers = original_auth_numbers - final_auth_numbers
        missing_rows = sources_df[sources_df['authorization_number'].isin(missing_auth_numbers)]

        # Rows added to final_df (if any)
        added_auth_numbers = final_auth_numbers - original_auth_numbers
        added_rows = final_df[final_df['authorization_number'].isin(added_auth_numbers)]

        if not missing_rows.empty:
            log.info("Rows missing from final_df:")
            log.info(missing_rows[['authorization_number', 'price_basis', 'drug_type']])
            utils.send_message_with_dataframe_to_slack(kwargs.get('temp_slack_channel'), kwargs.get('slack_bot_token'), f"Rows missing from final_df after joining chain_rates.\n", dataframe=missing_rows)

        if not added_rows.empty:
            log.info("Unexpected rows added to final_df:")
            log.info(added_rows[['authorization_number', 'price_basis', 'drug_type']])
            utils.send_message_with_dataframe_to_slack(kwargs.get('temp_slack_channel'), kwargs.get('slack_bot_token'), f"Unexpected rows added to final_df after joining chain_rates.\n", dataframe=added_rows)
    else:
        log.info("No mismatched rows found after joining chain_rates.")
    return final_df


def write_reconciliation_to_excel(report_dict, date_period, dispensing_fee_report_flag=True):
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
        # if 'TOTAL VARIANT AMOUNT' in report_dict:
        #     row += 1
        #     df = report_dict['TOTAL VARIANT AMOUNT']
        #     worksheet.write(row, 0, 'TOTAL VARIANT AMOUNT', formats['bold'])
        #     worksheet.write_row(row, 1, df.iloc[0].tolist(), formats['currency'])
        #     row += 1

        # Adjust column widths
        for col_idx, col_name in enumerate(df.columns):
            max_len = max(df[col_name].astype(str).str.len().max(), len(col_name)) + 2
            worksheet.set_column(col_idx, col_idx, max_len)

        writer.close()
        output.seek(0)
        return output


def create_report_dict(awp_results, nadac_results):
    price_basis_list = ['AWP', 'NADAC Default Rate', 'NADAC CIIs Rate']
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
        report_df = pd.DataFrame(columns=['Brand', 'Generic', 'Total Variant Amount'])

        if price_basis == 'AWP':
            price_basis_data = awp_results[awp_results['reconciliation_price_basis'] == price_basis]
        elif price_basis == 'NADAC Default Rate':
            price_basis_data = nadac_results[(nadac_results['reconciliation_price_basis'] == 'NADAC') & (nadac_results['claim_program_name'] == 'regular')]
        elif price_basis == 'NADAC CIIs Rate':
            price_basis_data = nadac_results[(nadac_results['reconciliation_price_basis'] == 'NADAC') & (nadac_results['claim_program_name'] == 'schedule_2')]
        else:
            continue

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
            report_df.at[f'{price_basis} Variant Amount', drug_type] = row['contracted_cost'] - row[
                'ingredient_cost_paid_resp']

        report_df.at[f'{price_basis} Variant Amount', 'Total Variant Amount'] = (
                report_df.at[f'{price_basis} Variant Amount', 'Brand'] +
                report_df.at[f'{price_basis} Variant Amount', 'Generic']
        )

        df_dict[f'{price_basis} Report'] = report_df

        # process dispensing fee block
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
        report_variant += df_dict[f'{price_basis} Claims'].at['Dispense Fee Variant Amount', 'Total Variant Amount']

        total_variant_amount_df.loc[price_basis] = [report_variant, 0]

    total_variant_sum = total_variant_amount_df.sum()
    final_variant_amount_df = pd.DataFrame(
        {'Generic': [total_variant_sum['Generic']], 'Brand': [total_variant_sum['Brand']],
         'Total': [total_variant_sum.sum()]},
        index=['TOTAL VARIANT AMOUNT']
    )

    df_dict['TOTAL VARIANT AMOUNT'] = final_variant_amount_df

    return df_dict
