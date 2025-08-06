import pandas as pd

from transform import pandas_helper

def join_cvs_quarterly_dataframes(claims_df: pd.DataFrame, source_dict: dict) -> pd.DataFrame:
    result_df=claims_df.copy()
    bin_019876_df = pandas_helper.left_join_with_condition(result_df[result_df.bin_number == '019876'], source_dict['excluded_ndcs'][source_dict['excluded_ndcs'].reason == 'cvs_exempt_generics'][['ndc', 'reason', 'valid_from', 'valid_to']], left_on='product_id', right_on='ndc').drop(columns=['ndc'])
    result_df = pd.concat([bin_019876_df, result_df[result_df.bin_number != '019876']])
    result_df = pandas_helper.left_join_with_condition(result_df, source_dict['cvs_tpdt_mac'][['ndc', 'mac', 'generic_product_identifier', 'valid_from', 'valid_to']], left_on='product_id', right_on='ndc', filter_by='valid_from_x').rename(columns={'generic_product_identifier':'mac_gpi'}).drop(columns=['ndc'])
    result_df = pandas_helper.left_join_with_condition(result_df, source_dict['ndcs_v2'][['id', 'is_otc', 'multi_source_code', 'valid_from', 'valid_to', 'name_type_code','nadac_is_generic']], left_on='product_id', right_on='id', filter_by='valid_from_x', normalize_valid_from=True)
    result_df = pandas_helper.left_join_with_condition(result_df, source_dict['ndc_costs_v2'][['awp', 'wac', 'ndc', 'valid_from', 'valid_to']], left_on='product_id', right_on='ndc', filter_by='valid_from_x', normalize_valid_from=True).drop(columns=['ndc'])
    bin_019901_df = pandas_helper.left_join_with_condition(result_df[result_df.bin_number == '019901'], source_dict['tpdt_specialty'][['ndc','valid_from','valid_to']], left_on='product_id', right_on='ndc')
    result_df = pd.concat([bin_019901_df, result_df[result_df.bin_number != '019901']]).rename(columns={'ndc':'ndc_specialty'})
    return result_df
