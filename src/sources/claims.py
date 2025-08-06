import pandas as pd
from decimal import Decimal
from transform import utils
from transform import pandas_helper
from hippo import logger

log = logger.getLogger("invoices claims sources")

def preprocess_claims(df, partners=False, chains=False, convert_dates_to_cst6cdt=False):
    default_columns = ['npi','valid_to','valid_from','claim_date_of_service','basis_of_reimbursement_determination_resp','total_paid_response','product_id','ingredient_cost_paid_resp','bin_number',
                    'authorization_number','process_control_number','group_id','prescription_reference_number','fill_number','quantity_dispensed','days_supply','usual_and_customary_charge',
                    'percentage_sales_tax_amount_paid','patient_pay_resp','dispensing_fee_paid_resp','transaction_code','dispense_as_written','network_reimbursement_id','rx_id','number_of_refills', 'amount_of_copay']
    if partners == True:
      default_columns.append('partner')
    if chains == True:
      default_columns.append('chain_name')
    claims_df = df[default_columns]

    claims_df['valid_to'] = claims_df['valid_to'].astype(str)
    claims_df['valid_to'].replace('2300-01-01 00:00:00', '2050-01-01 00:00:00', inplace=True)
    claims_df['valid_to'] = pd.to_datetime(claims_df['valid_to'],format='%Y-%m-%d %H:%M:%S')
    claims_df['valid_from'] = pd.to_datetime(claims_df['valid_from'],format='%Y-%m-%d %H:%M:%S')
    claims_df['days_supply'] = claims_df['days_supply'].fillna(30).astype(int) # replace null values with default DS value
    claims_df['claim_date_of_service'] = pd.to_datetime(claims_df['claim_date_of_service'],format='%Y-%m-%d %H:%M:%S')
    if convert_dates_to_cst6cdt == True: # allow to convert valid_from and valid_to to CST6CDT time zone that is default for Walgreens
      for column in ['valid_from','valid_to']:
        claims_df[column] = pd.to_datetime(claims_df[column]).dt.tz_localize('UTC')
        claims_df[column] = claims_df[column].dt.tz_convert('CST6CDT').dt.tz_localize(None)
    claims_df[['basis_of_reimbursement_determination_resp','bin_number']] = claims_df[['basis_of_reimbursement_determination_resp','bin_number']].astype('category')
    claims_df[['npi','product_id']] = claims_df[['npi','product_id']].astype('int64')
    claims_df  = utils.cast_columns_to_decimal(claims_df, column_names=['total_paid_response','ingredient_cost_paid_resp','percentage_sales_tax_amount_paid','usual_and_customary_charge','patient_pay_resp','dispensing_fee_paid_resp','quantity_dispensed'], fillna_flag = True)

    return claims_df

def join_default_source_dataframes(claims_df: pd.DataFrame, source_dict: dict) -> pd.DataFrame:
    if 'pharmacy' in source_dict and not source_dict['pharmacy'].empty:
        claims_df = pandas_helper.left_join_with_condition(
            claims_df,
            source_dict['pharmacy'][['id', 'chain_code', 'state_abbreviation', 'valid_from', 'valid_to', 'ncpdp']],
            left_on='npi', right_on='id'
        )
    
    if 'mf2ndc' in source_dict and not source_dict['mf2ndc'].empty:
        claims_df = claims_df.merge(
            source_dict['mf2ndc'][['ndc_upc_hri', 'drug_descriptor_id', 'dea_class_code']],
            how='left',
            left_on='product_id', right_on='ndc_upc_hri'
        )
    
    if 'mf2name' in source_dict and not source_dict['mf2name'].empty:
        claims_df = claims_df.merge(
            source_dict['mf2name'][['drug_name', 'drug_descriptor_id', 'generic_product_identifier']],
            how='left',
            on='drug_descriptor_id'
        )

    # Concatenate Kroger MAC datasets before joining
    if 'kroger_cs_2_mac' in source_dict and not source_dict['kroger_cs_2_mac'].empty:
        source_dict['kroger_mac'] = source_dict['kroger_cs_2_mac'].copy()

    if 'kroger_cs_3_4_5_mac' in source_dict and not source_dict['kroger_cs_3_4_5_mac'].empty:
        if 'kroger_mac' in source_dict and not source_dict['kroger_mac'].empty:
            source_dict['kroger_mac'] = pd.concat(
                [source_dict['kroger_cs_2_mac'], source_dict['kroger_cs_3_4_5_mac']]
            )
        else:
            source_dict['kroger_mac'] = source_dict['kroger_cs_3_4_5_mac'].copy()

    if 'kroger_mac' in source_dict and not source_dict['kroger_mac'].empty:

        claims_df = pandas_helper.left_join_with_condition(
            claims_df,
            source_dict['kroger_mac'][['ndc', 'mac', 'valid_from', 'valid_to']],
            left_on='product_id', right_on='ndc'
        ).drop(columns=['ndc'])

    if 'ndcs_v2' in source_dict and not source_dict['ndcs_v2'].empty:
        claims_df = pandas_helper.left_join_with_condition(
            claims_df,
            source_dict['ndcs_v2'][['id', 'is_otc', 'multi_source_code', 'valid_from', 'valid_to', 'name_type_code','nadac_is_generic']],
            left_on='product_id', right_on='id'
        )

    if 'ndc_costs_v2' in source_dict and not source_dict['ndc_costs_v2'].empty:
        claims_df = pandas_helper.left_join_with_condition(
            claims_df,
            source_dict['ndc_costs_v2'][['awp', 'wac', 'nadac', 'gpi_nadac', 'ndc', 'valid_from', 'valid_to']],
            left_on='product_id', right_on='ndc'
        ).drop(columns=['ndc'])

    if 'claim_processing_fees' in source_dict and not source_dict['claim_processing_fees'].empty:
        claims_df = claims_df.merge(
            source_dict['claim_processing_fees'][['processor_fee', 'valid_from', 'rx_id']],
            how='left',
            on=['valid_from', 'rx_id']
        )

    if 'drug_ndcs_with_rxnorm' in source_dict and not source_dict['drug_ndcs_with_rxnorm'].empty:
        claims_df = pandas_helper.left_join_with_condition(
            claims_df,
            source_dict['drug_ndcs_with_rxnorm'][['ndc', 'route_of_administration', 'valid_from', 'valid_to']],
            left_on='product_id', right_on='ndc'
        )

    return claims_df

def join_chain_rates(sources_df, chain_rates_df, column_to_merge='drug_type', **kwargs):
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
        columns_transactions = ['bin_number', column_to_merge, 'reconciliation_price_basis']
        columns_chain_rates = ['bin_number', 'drug_type', 'price_basis']
        if 'program_name' in non_unc_transactions.columns:
            columns_transactions.append('program_name')
            columns_chain_rates.append('program_name')
        check_value_combination_before_join_chain_rates(non_unc_transactions, chain_rates_df, column_to_merge) # run the test to ensure all distinct values from source_dict merging columns presented in chain_rates_df as well
        merged_df = pd.merge(
            non_unc_transactions,
            chain_rates_df,
            how='left',
            left_on=columns_transactions,
            right_on=columns_chain_rates,
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
            utils.send_message_with_dataframe_to_slack(kwargs.get('temp_slack_channel'), kwargs.get('slack_bot_token'), f"@echo Rows missing from final_df after joining chain_rates.\n", dataframe=missing_rows)

        if not added_rows.empty:
            log.info("Unexpected rows added to final_df:")
            log.info(added_rows[['authorization_number', 'price_basis', 'drug_type']])
            utils.send_message_with_dataframe_to_slack(kwargs.get('temp_slack_channel'), kwargs.get('slack_bot_token'), f"@echo Unexpected rows added to final_df after joining chain_rates.\n", dataframe=added_rows)
    else:
        log.info("No mismatched rows found after joining chain_rates.")
    return final_df

def check_value_combination_before_join_chain_rates(sources_df, chain_rates_df, column_to_merge):
    # Check for bin_number
    missing_bin_numbers = set(sources_df['bin_number'].unique()) - set(chain_rates_df['bin_number'].unique())
    assert not missing_bin_numbers, f"Missing bin_numbers in chain_rates_df: {missing_bin_numbers}"
    
    # Check for column_to_merge (assuming 'column_to_merge' is 'drug_type')
    missing_column_to_merge = set(sources_df[column_to_merge].unique()) - set(chain_rates_df['drug_type'].unique())
    assert not missing_column_to_merge, f"Missing values in {column_to_merge} in chain_rates_df: {missing_column_to_merge}"
    
    # Check for price_basis
    missing_price_basis = set(sources_df['reconciliation_price_basis'].unique()) - set(chain_rates_df['price_basis'].unique())
    assert not missing_price_basis, f"Missing price_basis values in chain_rates_df: {missing_price_basis}"

def test_invoice_results_vs_transaction_final(invoice_results, transaction_final_df):
    """
    Ensure that the SUM of the values in ingredient_cost_paid_resp, total_paid_response, and claims count 
    in invoice_results are equal to the sum of the respective columns in transaction_final_df.
    """
    # Calculate sums in invoice_results
    invoice_ingredient_sum = invoice_results['ingredient_cost_paid_resp'].sum()
    invoice_total_paid_sum = invoice_results['total_paid_response'].sum()
    invoice_claims_count = invoice_results['claims count'].sum()

    # Calculate corresponding sums in transaction_final_df
    transaction_ingredient_sum = transaction_final_df['ingredient_cost_paid_resp'].sum()
    transaction_total_paid_sum = transaction_final_df['total_paid_response'].sum()
    transaction_claims_count = transaction_final_df['fill_reversal_indicator'].sum()

    # Perform the assertions
    assert invoice_ingredient_sum == transaction_ingredient_sum, (
        f"Mismatch in ingredient cost: "
        f"invoice_results={invoice_ingredient_sum}, transaction_final_df={transaction_ingredient_sum}"
    )
    assert invoice_total_paid_sum == transaction_total_paid_sum, (
        f"Mismatch in total paid response: "
        f"invoice_results={invoice_total_paid_sum}, transaction_final_df={transaction_total_paid_sum}"
    )
    assert invoice_claims_count == transaction_claims_count, (
        f"Mismatch in claims count: "
        f"invoice_results={invoice_claims_count}, transaction_final_df={transaction_claims_count}"
    )


def join_ndcs_data(raw_claims_df, raw_ndchist_df):
    claims_pharmacy_history_ndchistory_df = pandas_helper.left_join_with_condition(raw_claims_df, raw_ndchist_df[['id','is_otc','multi_source_code','valid_from','valid_to']], left_on='product_id', right_on='id')
    return claims_pharmacy_history_ndchistory_df

def filter_reversals(df, period_start, period_end):
  df = df[~((df.valid_to.dt.date >= period_start.date()) & (df.valid_to.dt.date <= period_end.date()) & (df.valid_from.dt.date >= period_start.date()))] # if reversals included - filter by valid_from::date < period_start::date
  return df
