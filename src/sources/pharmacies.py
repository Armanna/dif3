import pandas as pd
import numpy as np

class Pharmacies():
    CHAIN_CODES = []
    BIN_NUMBERS = []
    CHAIN_NAME = []
    
    def __init__(self, chain_code=CHAIN_CODES, bin_nubmers=BIN_NUMBERS, chain_name=CHAIN_NAME):
        self.chain_codes = chain_code
        self.bin_nubmers = bin_nubmers
        self.chain_name = chain_name

class CVS(Pharmacies):
    CHAIN_CODES=['008','039','123','177','207','673']
    BIN_NUMBERS=['019876','019901','027836']
    NET_NUMBER = 30
    DUE_DATE = 30
    INVOICE_STATIC_NUMBER = 21030

    def add_brand_generic_indicator_invoice(self, df) -> pd.Series:
        brand_generic_column = df.apply(lambda df_row: 
            'generic' if df_row.multi_source_code == 'Y' or df_row.dispense_as_written == 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' else
            'brand', axis=1)
        return brand_generic_column
    
    def add_brand_generic_indicator(df) -> pd.Series:
        def get_drug_type(df_row):
            if df_row.claim_date_of_service < pd.to_datetime('2024-04-01'):
                # Before CostVantage logic 
                if df_row.reason == 'cvs_exempt_generics':
                    return 'exempt generic'
                elif df_row.multi_source_code == 'Y' and df_row.is_otc == 'True' and pd.isna(df_row['ndc_specialty']):
                    return 'OTC generic'
                elif df_row.dispense_as_written == 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC':
                    return 'generic'
                elif df_row.multi_source_code == 'Y' and df_row.is_otc == 'False' and pd.isna(df_row['ndc_specialty']):
                    return 'generic'
                elif df_row.multi_source_code == 'Y' and not pd.isna(df_row['ndc_specialty']):
                    return 'Specialty generic'
                elif df_row.multi_source_code != 'Y' and df_row.is_otc == 'True' and pd.isna(df_row['ndc_specialty']):
                    return 'OTC brand'
                else:
                    return 'brand'
            else:
                # CostVantage logic
                if df_row.reason == 'cvs_exempt_generics':
                    return 'exempt generic'
                elif df_row.bin_number == '019901' and df_row.name_type_code == 'G':
                    return 'generic'
                elif df_row.bin_number in ['019876', '027836'] and df_row.claim_date_of_service >= pd.to_datetime('2024-07-01') and df_row.name_type_code == 'G':
                    return 'generic'
                elif df_row.dispense_as_written == 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' or df_row.multi_source_code == 'Y':
                    return 'generic'
                else:
                    return 'brand'

        drug_type_column = df.apply(get_drug_type, axis=1)
        return drug_type_column

    def define_reconciliation_price_basis(df: pd.DataFrame) -> pd.Series:
        return df['price_basis']

    def assign_line_of_business(df: pd.DataFrame) -> pd.Series:
        """
        CVS told us that TPDM and Regualar must be reconciled as the same line of business.
        That means that currently we have:
        PORTAL - TPDT;
        DTC - TPDM + REGULAR;
        INTEGRATION - currently it's only Waltz, but in the future it could be more.
        """
        conditions = [
            (df['bin_number'] == '019901'),
            (df['bin_number'] == '019876') & (df['network_reimbursement_id'] == '2274'),
            (df['bin_number'] == '019876') & (df['network_reimbursement_id'] != '2274'),
            (df['bin_number'] == '027836')
        ]
        choices = ['PORTAL', 'DTC', 'INTEGRATION', 'DTC']
        return np.select(conditions, choices, default='UNKNOWN')


class Walgreens(Pharmacies):
    CHAIN_CODES=['226','A10','A13']
    BIN_NUMBERS=[]
    NET_NUMBER = 15
    DUE_DATE = 15
    INVOICE_STATIC_NUMBER = 41030

    def add_brand_generic_indicator(self, df) -> pd.Series:
        brand_generic_column = df.apply(lambda df_row: 
            'generic' if df_row.multi_source_code == 'Y' else
            'brand', axis=1)
        return brand_generic_column

class KrogerAndHarrisTeeter(Pharmacies):
    KROGER_CHAIN_CODES = [
    '043', '069', '071', '108', '113',
    '199', '273', '495', '817', 'B67', 'A65'
    ]
    HARRIS_TEETER_CHAIN_CODES = [
    '602'
    ]
    BIN_NUMBERS=['019876']
    NET_NUMBER = '30'
    DUE_DATE = 30
    INVOICE_STATIC_NUMBER = 61030 
    CONTRACT_START_DATE = pd.to_datetime('2024-05-15')

    TARGET_GER = '0.2'
    TARGET_BER = '0.13'
    TARGET_AFER = '4'
    DISP_FEE_DICT = {
        'Non-controlled Drug':{
            'brand':{
                '1-83': 15,
                '84+': 22.5
            },
            'generic':{
                '1-83': 12,
                '84+': 15
            },
        },
        'Controlled Drug C-III - C-V':{
            'brand':{
                '1-83': 22.5,
                '84+': 32.5
            },
            'generic':{
                '1-83': 16,
                '84+': 20
            },
        },
        'Controlled Drug C-II':{
            'brand':{
                '1-83': 25,
                '84+': 35
            },
            'generic':{
                '1-83': 20,
                '84+': 25
            },
        },
        'AWP':{
            'brand':{
                '1-83': 5,
                '84+': 5
            },
            'generic':{
                '1-83': 5,
                '84+': 5
            }
        }
    }


    COMPANY_NAME = "The Kroger Co."
    ADDR_LINE_2 = "Attn: Contracting & Analytics"
    ADDR_LINE_3 = "555 Race Street"
    ADDR_LINE_4 = "Cincinnati, OH 45202"

    COLUMN_RENAME_DICT = {
        'bin_number': 'BIN',
        'process_control_number': 'PCN',
        'network_reimbursement_id': 'Network Reimbursement ID',
        'authorization_number': 'Claim number',
        'claim_date_of_service': 'Date of Service',
        'valid_from': 'Fill date',
        'fill_number': 'Refill number',
        'drug_name': 'Drug name',
        'quantity_dispensed': 'Total quantity dispensed',
        'npi': 'NPI',
        'days_supply': "Days' supply",
        'prescription_reference_number': 'Prescription number',
        'total_paid_response': 'Administration Fee',
        'ingredient_cost_paid_resp': 'Paid Ingredient Cost',
        'dispensing_fee_paid_resp': 'Paid Dispense Fee',
        'patient_pay_resp': 'Patient Paid Amount',
        'multi_source_code': 'MONY code',
        'product_id': 'NDC',
        'rx_id': 'Claim reference number',
        'usual_and_customary_charge': 'Usual and Customary Charge (U&C)',
        'dea_class_code': 'DEA Class Code',
        'ncpdp': 'NCPDP',
        'awp': 'AWP',
        'mac': 'MAC',
        'nadac': 'NADAC',
        'percentage_sales_tax_amount_paid': 'Tax paid'
    }

    def add_brand_generic_indicator(series: pd.Series) -> pd.Series:
        brand_generic_series = series.apply(lambda x: 'generic' if x == 'Y' else 'brand')
        return brand_generic_series

    def determine_dea_claim_type(dea_class_code_column: pd.Series) -> pd.Series:
        mapping = {
            '2': 'Controlled Drug C-II',
            '3': 'Controlled Drug C-III - C-V',
            '4': 'Controlled Drug C-III - C-V',
            '5': 'Controlled Drug C-III - C-V',
        }
        result_series = pd.Series('Non-controlled Drug', index=dea_class_code_column.index) # set 'Non-controlled Drug' as a default value before mapping
        result_series.update(dea_class_code_column.map(mapping))

        return result_series

    def set_exclusion_flags(row):
        basis_of_reimbursement_determination_resp = row.get('basis_of_reimbursement_determination_resp', None)
        is_otc = row.get('is_otc', None)
        route_of_administration = row.get('route_of_administration', None)

        if pd.isna(route_of_administration):
            route_of_administration = ''
        else:
            route_of_administration = str(route_of_administration)

        if basis_of_reimbursement_determination_resp == '04':
            return 'Y', 'UNC'
        elif is_otc == 'True':
            return 'Y', 'OTC'
        elif route_of_administration in ('IJ', 'SC', 'IA', 'ID', 'IM'):
            return 'Y', 'INJECTABLE'
        else:
            return 'N', ''


class Publix(Pharmacies):
    CHAIN_CODES = ['302']
    BIN_NUMBERS = ['019876', '026465']
    NET_NUMBER = '60'
    DUE_DATE = 60
    INVOICE_STATIC_NUMBER = 71030
    CONTRACT_START_DATE = pd.to_datetime('2024-08-01')
    MARKETPLACE_CONTRACT_START_DATE = pd.to_datetime('2025-03-31')

    COMPANY_NAME = "Publix Super Markets, Inc."
    ADDR_LINE_2 = "3300 Publix Corporate Parkway"
    ADDR_LINE_3 = "Lakeland, FL 33811-3311"

    COLUMN_RENAME_DICT = {
        'bin_number': 'BIN',
        'process_control_number': 'PCN',
        'rx_id': 'Claim reference number',
        'prescription_reference_number': 'Prescription number',
        'fill_number': 'Refill number',
        'claim_date_of_service': 'Date of Service',
        'reconciliation_price_basis': 'Basis of reimbursement source',
        'claim_program_name': 'Control Schedule',
        'npi': 'NPI',
        'patient_pay_resp': 'Patient Paid Amount',
        'total_paid_response': 'Administration Fee',
        'quantity_dispensed': 'Total quantity dispensed',
        'days_supply': "Days' supply",
        'ingredient_cost_paid_resp': 'Paid Ingredient Cost',
        'dispensing_fee_paid_resp': 'Paid Dispense Fee',
        'usual_and_customary_charge': 'Usual and Customary Charge (U&C)',
        'percentage_sales_tax_amount_paid': 'Tax paid',
        'ncpdp': 'NCPDP',
        'valid_from': 'Paid claim response date',
        'awp': 'Total AWP',
        'nadac': 'Total NADAC',
        'wac': 'Total WAC',
        'drug_type': 'Brand/Generic Medi-Span indicator field',
        'processor_fee': 'Transmission porcessing fee',
        'network_reimbursement_id': 'Plan carrier code',
        'Exclusion reason': 'Excluded Reason'
    }

    def add_brand_generic_indicator(df: pd.DataFrame) -> pd.DataFrame:
        def _apply_brand_generic_indicator(row: pd.Series) -> str:
            if row['multi_source_code'] == 'Y':
                return 'generic'
            else:
                return 'brand'
        return df.apply(_apply_brand_generic_indicator, axis=1)

    def add_program_flag(df: pd.DataFrame) -> pd.DataFrame:
        def _apply_program_flag(row: pd.Series) -> str:
            if row['dea_class_code'] == '2':
                return 'schedule_2'
            else:
                return 'regular'
        return df.apply(_apply_program_flag, axis=1)

    def define_reconciliation_price_basis(df: pd.DataFrame) -> pd.Series:
        def _apply_price_basis_type(row: pd.Series) -> str:

            # in rare cases there is no value provided for nadac & gpi_nadac & awp
            if pd.isna(row['nadac']) and pd.isna(row['wac']) and pd.isna(row['awp']):
                return 'UNC'
            # no need to recalculate basis if it's UNC
            if row['price_basis'] == 'UNC':
                return 'UNC'
            if row['drug_type'] == 'generic':
                if pd.notna(row['nadac']):
                    return 'NADAC'
                elif row['bin_number'] == '019876' and pd.notna(row['wac']):
                    return 'WAC'
                elif row['bin_number'] == '026465' and pd.notna(row['awp']):
                    return 'AWP'
                else:
                    return 'UNC'
            elif row['drug_type'] == 'brand':
                return 'AWP'

        return df.apply(_apply_price_basis_type, axis=1)

    def set_exclusion_flags(row):
        basis_of_reimbursement_determination_resp = row.get('basis_of_reimbursement_determination_resp', None)
        is_otc = row.get('is_otc', None)

        if basis_of_reimbursement_determination_resp == '04':
            return 'Y', 'UNC'
        elif is_otc == 'True':
            return 'Y', 'OTC'
        else:
            return 'N', ''

class Walmart(Pharmacies):
    CHAIN_CODES = ['229']
    BIN_NUMBERS=['019876']
    NET_NUMBER = '60'
    DUE_DATE = 60
    INVOICE_STATIC_NUMBER = 71031 
    CONTRACT_START_DATE = pd.to_datetime('2024-09-03')

    COMPANY_NAME = "Walmart Inc."
    ADDR_LINE_2 = "2608 S.E. J Street, Mail Stop 0440"
    ADDR_LINE_3 = "Bentonville, Arkansas 72716-0440"

    ADMIN_FEE = '4.5'

    COLUMN_RENAME_DICT = {
        'chain_code': 'Chain Code',
        'npi': 'NPI',
        'authorization_number': 'Claim Authorization ID',
        'prescription_reference_number': 'Prescription Number',
        'claim_date_of_service': 'Fill Date',
        'valid_from': 'Adjudication Date',
        'number_of_refills': 'Refill Number',
        'bin_number': 'BIN',
        'process_control_number': 'PCN',
        'network_reimbursement_id': 'Network ID',
        'quantity_dispensed': 'Total Quantity Dispensed',
        'days_supply': "Total Day Supply Dispensed",
        'drug_type': 'Brand/Generic Designation',
        'multi_source_code': 'Multi-Source Indicator',
        'name_type_code': 'Medi-Span Drug Type Name',
        'generic_product_identifier': 'GPI',
        'dispense_as_written': 'DAW Code',
        'target_rate': 'Contracted Rate',
        'dfer_target_rate': 'Contracted Dispense Fee',
        'reconciliation_price_basis': 'Reimbursement Type',
        'ingredient_cost_paid_resp': 'Ingredient Cost Paid',
        'usual_and_customary_charge': 'U&C Amount',
        'patient_pay_resp': 'Patient Pay Response',
        'dispensing_fee_paid_resp': 'Dispensing Fee Paid',
        'percentage_sales_tax_amount_paid': 'Taxes Paid',
        'total_paid_response': 'Administration Fee',
        'Exclusion reason': 'Excluded Reason',
        'awp': 'AWP',
        'nadac': 'NADAC',
        'gpi_nadac': 'GPI_NADAC'
    }

    REQUESTED_COLUMNS = [
        "Chain Code",
        "NPI",
        "Claim Authorization ID",
        "Prescription Number",
        "Fill Date",
        "Reversal date",
        "Adjudication Date",
        "BIN",
        "PCN",
        "Network ID",
        "Network",
        "30/90 Day Claim Indicator",
        "Total Quantity Dispensed",
        "Total Day Supply Dispensed",
        "Plan Category",
        "Brand/Generic Designation",
        "Multi-Source Indicator",
        "Medi-Span Drug Type Name",
        "GPI",
        "NDC",
        "DAW Code",
        "U&C Indicator",
        "Unique Drug Indicator",
        "Contracted Rate",
        "Contracted Dispense Fee",
        "Contracted Vaccine Administration Fee",
        "Reimbursement Type",
        "Actual Rate",
        "Total Cost Share Paid",
        "AWP",
        'NADAC',
        'GPI_NADAC',
        "Ingredient Cost Paid",
        "U&C Amount",
        "Patient Pay Response",
        "Dispensing Fee Paid",
        "Administration Fee",
        "Vaccine Administration Fee Paid",
        "Taxes Paid",
        "Variant Amount",
        "Excluded Claim Indicator",
        "Excluded Reason",
        "Fill/Reversal indicator"
    ]

    def add_brand_generic_indicator(df: pd.DataFrame) -> pd.Series:
        def _apply_brand_generic_indicator(row: pd.Series) -> str:
            if row['nadac_is_generic'] == 'True' and pd.notna(row['nadac']):
                return 'generic'
            elif pd.isna(row['nadac']) and row['multi_source_code'] == 'Y':
                return 'generic'
            else:
                return 'brand'
        return df.apply(_apply_brand_generic_indicator, axis=1)

    def define_reconciliation_price_basis(df: pd.DataFrame) -> pd.Series:
        def _apply_price_basis_type(row: pd.Series) -> str:
            
            # in rare cases there is no value provided for nadac & gpi_nadac & awp
            if pd.isna(row['nadac']) and pd.isna(row['gpi_nadac']) and pd.isna(row['awp']):
                return 'UNC'
            # no need to recalculate basis if it's UNC
            if row['price_basis'] == 'UNC': 
                return 'UNC'
            if row['drug_type'] == 'generic':
                if pd.notna(row['nadac']) or pd.notna(row['gpi_nadac']):
                    return 'NADAC'
                elif pd.isna(row['nadac']) and pd.isna(row['gpi_nadac']):
                    return 'AWP'
            elif row['drug_type'] == 'brand':
                return 'AWP'
            
        return df.apply(_apply_price_basis_type, axis=1)

    def set_exclusion_flags(row):
        basis_of_reimbursement_determination_resp = row.get('basis_of_reimbursement_determination_resp', None)
        four_dol_ndc_flag = row.get('four_dol_ndc_flag', None)
        reason = row.get('reason', None)

        if four_dol_ndc_flag:
            return 'Y', '$4 LIST UNC'
        elif basis_of_reimbursement_determination_resp == '04':
            return 'Y', 'UNC'
        elif pd.notna(reason):
            return 'Y', reason
        else:
            return 'N', ''

class Meijer(Pharmacies):
    CHAIN_CODES = ['213']
    BIN_NUMBERS=['019876']
    NET_NUMBER = '60'
    DUE_DATE = 60
    INVOICE_STATIC_NUMBER = 71032 
    CONTRACT_START_DATE = pd.to_datetime('2025-01-01')

    COMPANY_NAME = "Meijer, Inc."
    ADDR_LINE_2 = "2929 Walker Ave NW Grand Rapids, MI 49544"
    ADDR_LINE_3 = ""
    TARGET_ANR = "7.5"
    TARGET_AFER = "5.5"

    COLUMN_RENAME_DICT = {
        'bin_number': 'BIN',
        'process_control_number': 'PCN',
        'group_id': 'GROUP',
        'npi': 'NPI',
        'ncpdp': 'NCPDP',
        'network_reimbursement_id': 'NRID',
        'rx_id': 'Rx#',
        'claim_date_of_service': 'Date of Service',
        'drug_name': 'Drug Name',
        'price_basis': 'Basis of reimbursement source',
        'prescription_reference_number': 'Prescription number',
        'fill_number': 'Refill number',
        'drug_type': 'Generic Brand or Multi-Source indicator',
        'quantity_dispensed': 'Total quantity dispensed',
        'days_supply': "Total days supply",
        'usual_and_customary_charge': 'Dispensing pharmacy U&C Price',
        'percentage_sales_tax_amount_paid': 'Taxes paid',
        'total_paid_response': 'Administration Fee',
        'patient_pay_resp': 'Customer Payment amount',
        'nadac': 'NADAC',
        'awp': 'AWP',
        'ingredient_cost_paid_resp': 'Ingredient Cost paid',
        'dispensing_fee_paid_resp': 'Dispensing Fee paid',
        'reversal indicator': 'Reversal indicator'
    }

    REQUESTED_COLUMNS = [
        'BIN+PCN+GROUP',
        'NPI',
        'NCPDP',
        'NRID',
        'Claim Authorization Number',
        'Rx#',
        'Date of Service',
        'Fill date',
        'NDC',
        'Drug Name',
        'Basis of reimbursement source',
        'Prescription number',
        'Refill number',
        'Generic Brand or Multi-Source indicator',
        'Total quantity dispensed',
        'Total days supply',
        'Dispensing pharmacy U&C Price',
        'Taxes paid',
        'Administration Fee',
        'Customer Payment amount',
        'NADAC',
        'AWP',
        'Ingredient Cost paid',
        'Dispensing Fee paid',
        'Excluded Claim Indicator',
        'Exclusion reason',
        'Reversal indicator',
        'Reversal date'
    ]


    def add_brand_generic_indicator(df: pd.DataFrame) -> pd.DataFrame:
        def _apply_brand_generic_indicator(row: pd.Series) -> str:
            if row['multi_source_code'] == 'Y':
                return 'generic'
            else:
                return 'brand'
        return df.apply(_apply_brand_generic_indicator, axis=1)

    def define_reconciliation_price_basis(df: pd.DataFrame) -> pd.Series:
        def _apply_price_basis_type(row: pd.Series) -> str:
            
            # in rare cases there is no value provided for nadac & awp
            if pd.isna(row['nadac']) and pd.isna(row['awp']):
                return 'UNC'
            # no need to recalculate basis if it's UNC
            if row['price_basis'] == 'UNC': 
                return 'UNC'
            # if NADAC cost available then NADAC
            if pd.notna(row['nadac']):
                return 'NADAC'
            # if there is no NADAC available then AWP
            if pd.isna(row['nadac']):
                return 'AWP'
            
        return df.apply(_apply_price_basis_type, axis=1)

    def set_exclusion_flags(row):
        basis_of_reimbursement_determination_resp = row.get('basis_of_reimbursement_determination_resp', None)
        is_otc = row.get('is_otc', None)
        if basis_of_reimbursement_determination_resp == '04':
            return 'Y', 'UNC'
        elif is_otc == 'True':
            return 'Y', 'OTC'
        else:
            return 'N', ''

    @classmethod
    def _select_fields_for_exporting(cls, transactions_df):
        transactions_df['BIN+PCN+GROUP'] = transactions_df['bin_number'].astype('object').fillna('') + '+' + \
                                           transactions_df['process_control_number'].fillna('') + '+' + transactions_df[
                                               'group_id'].fillna('')
        transactions_df['Claim Authorization Number'] = '="' + transactions_df['authorization_number'].fillna(
            '') + '"'  # needed for excel to treat data as text not number
        transactions_df['product_id'] = transactions_df['product_id'].astype('string').str.zfill(11)
        transactions_df['NDC'] = '="' + transactions_df['product_id'].fillna(
            '') + '"'  # needed for excel to treat data as text not number
        transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna(
            '')
        transactions_df['Fill date'] = transactions_df['valid_from'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna('')
        transactions_df['Reversal date'] = transactions_df['valid_to'].apply(lambda x: x.strftime('%Y-%m-%d')).fillna(
            '')
        transactions_df['claim_date_of_service'] = transactions_df['claim_date_of_service'].apply(
            lambda x: x.strftime('%Y-%m-%d')).fillna('')
        transactions_df['Reversal date'] = np.where(transactions_df['reversal indicator'] != 'B2', '', transactions_df[
            'Reversal date'])  # set empty string instead of 2050-01-01 for fills
        transactions_df = transactions_df.rename(columns=cls.COLUMN_RENAME_DICT)  # rename columns
        transactions_df = transactions_df[cls.REQUESTED_COLUMNS]

        return transactions_df

class Albertsons(Pharmacies):
    CHAIN_CODES = ['003', '027', '156', '158', '227', '282', '301', '319', '400', '929', 'B62', 'C08', 'C31']
    BIN_NUMBERS=['019876']
    NET_NUMBER = '30'
    DUE_DATE = 30
    REVENUE_SHARE_OVER_MARGIN_PERCENT = '0.5'
    ADMIN_FEE_SHARE_FLOR = '5'
    INVOICE_STATIC_NUMBER = 710338
    CONTRACT_START_DATE = pd.to_datetime('2025-04-01')

    COMPANY_NAME = "Albertsons Companies, Inc."
    ADDR_LINE_2 = "250 E. Parkcenter Blvd"
    ADDR_LINE_3 = "Boise, ID 83706"

    COLUMN_RENAME_DICT = {
        'bin_number': 'BIN',
        'process_control_number': 'PCN',
        'group_id': 'GROUP',
        'npi': 'NPI',
        'ncpdp': 'NCPDP',
        'network_reimbursement_id': 'NRID',
        'claim_date_of_service': 'Date of Service',
        'drug_name': 'Drug Name',
        'price_basis': 'Basis of reimbursement source',
        'prescription_reference_number': 'Rx#',
        'fill_number': 'Refill number',
        'drug_type': 'Generic Brand or Multi-Source indicator',
        'quantity_dispensed': 'Total quantity dispensed',
        'days_supply': "Total days supply",
        'usual_and_customary_charge': 'Dispensing pharmacy U&C Price',
        'percentage_sales_tax_amount_paid': 'Taxes paid',
        'total_paid_response': 'Administration Fee',
        'patient_pay_resp': 'Member Payment amount',
        'nadac': 'NADAC',
        'awp': 'AWP',
        'ingredient_cost_paid_resp': 'Ingredient Cost paid',
        'dispensing_fee_paid_resp': 'Dispensing Fee paid',
        'reversal indicator': 'Reversal indicator'
    }

    REQUESTED_COLUMNS = [
        'BIN+PCN+GROUP',
        'NRID',
        'NPI',
        'NCPDP',
        'Claim Authorization Number',
        'Rx#',
        'Date of Service',
        'Fill date',
        'Refill number',
        'NDC',
        'Drug Name',
        'Generic Brand or Multi-Source indicator',
        'Total quantity dispensed',
        'Total days supply',
        'Dispensing pharmacy U&C Price',
        'Taxes paid',
        'Administration Fee',
        'Revenue Share',
        'Member Payment amount',
        'NADAC Indicator',
        'NADAC',
        'AWP',
        'Ingredient Cost paid',
        'Dispensing Fee paid',
        'Excluded Claim Indicator',
        'Exclusion reason',
        'Reversal indicator',
        'Reversal date'
    ]

    def add_brand_generic_indicator(df: pd.DataFrame) -> pd.Series:
        is_brand_daw = {
            'SUBSTITUTION NOT ALLOWED BY PRESCRIBER',
            'SUBSTITUTION ALLOWED PATIENT REQUESTED PRODUCT DISPENSED',
            'SUBSTITUTION ALLOWED BY PRESCRIBER BUT PLAN REQUESTS BRAND PATIENT PLAN REQUESTED BRAND PRODUCT'
        }

        mask_generic = (
            (df['nadac_is_generic'] == 'True') |
            (df['nadac_is_generic'].isnull() & (df['multi_source_code'] == 'Y')) |
            (
                df['nadac_is_generic'].isnull()
                & (df['multi_source_code'] == 'M')
                & (df['name_type_code'] == 'G')
            ) |
            (
                df['nadac_is_generic'].isnull()
                & (df['multi_source_code'] == 'O')
                & (df['name_type_code'] == 'G')
                & (~df['dispense_as_written'].isin(is_brand_daw))
            )
        )

        return pd.Series(np.where(mask_generic, 'generic', 'brand'), index=df.index)

    def define_reconciliation_price_basis(df: pd.DataFrame) -> pd.Series:
        def _apply_price_basis_type(row: pd.Series) -> str:

            # in rare cases there is no value provided for nadac & awp
            if pd.isna(row['nadac']) and pd.isna(row['awp']):
                return 'UNC'
            # no need to recalculate basis if it's UNC
            if row['price_basis'] == 'UNC': 
                return 'UNC'
            # if NADAC cost available then NADAC
            if pd.notna(row['nadac']):
                return 'NADAC'
            # if there is no NADAC available then AWP
            if pd.isna(row['nadac']):
                return 'AWP'

        return df.apply(_apply_price_basis_type, axis=1)
    
    def set_exclusion_flags(row):
        basis_of_reimbursement_determination_resp = row.get('basis_of_reimbursement_determination_resp', None)
        if basis_of_reimbursement_determination_resp == '04':
            return 'Y', 'UNC'
        else:
            return 'N', ''
