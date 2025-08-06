with date_range as (
select 
  dateadd('month',-1,date_trunc('month',current_date))::date AS period_start,
  (DATE_TRUNC('month', current_timestamp) - Interval '1 day')::date AS period_end
),
-- with date_range as (
-- select  
--     '20210501'::date as period_start,
--     '20210531'::date as period_end,
--     current_timestamp as now
-- ),
fills as (
SELECT
  DISTINCT
  c.bin_number || '+' || c.process_control_number AS "BIN+PCN",
  c.bin_number AS "Unique Identifier",
  c.npi AS "NPI",
  '="' || c.authorization_number || '"' AS "Claim Authorization Number", --needed for excel to treat data as text not number
  c.prescription_reference_number AS "Rx#",
  c.claim_date_of_service::text AS "Service Date",
  date(c.valid_from)::text as "Fill Date",
  '' as "Reversal Date",
  c.fill_number AS "Refill number",
  --c.response_time::date AS "Date of Fill", --use ETC date and claim valid time
  '="' || c.product_id || '"' AS "NDC", --needed for excel to treat data as text not number
  mfnm.drug_name AS "Drug Name",
  case when     
      ((ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') OR 
      (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != ''))    
      then 'nadac'
    else 'awp' end as fill_type,
  CASE 
    WHEN fill_type = 'awp' 
        AND ndcsh.multi_source_code = 'Y' 
        THEN 'generic'
    WHEN fill_type = 'nadac'        
        THEN 'generic'
    ELSE 'brand'
  END AS "Generic Indicator (Multi-Source Indicator)",
  c.quantity_dispensed AS "Total Quantity Dispensed",
  c.days_supply AS "Total days supply",
  c.usual_and_customary_charge::BIGINT / 100::decimal(3, 0) AS "Dispensing pharmacy U&C Price",
  ISNULL(c.percentage_sales_tax_amount_paid, 0)::BIGINT / 100::decimal(3,0) AS "Taxes Paid",
  -c.total_paid_response::BIGINT / 100::decimal(3, 0) AS "Administration Fee",
  c.patient_pay_resp::BIGINT / 100::decimal(3, 0) AS "Patient Amount",
  ROUND(NULLIF(nch.awp, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Average wholesale price (AWP)",
  ROUND(COALESCE(NULLIF(nch.nadac, ''), NULLIF(nch.gpi_nadac, ''))::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Contracted NADACTR",
  ROUND(NULLIF(nch.wac, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Wholesale acquisition cost (WAC)",
  c.ingredient_cost_paid_resp::BIGINT / 100::decimal(3, 0) AS "Ingredient cost paid",
  c.dispensing_fee_paid_resp::BIGINT / 100::decimal(3, 0) AS "Dispensing fee paid",
  CASE
    WHEN c.usual_and_customary_charge::BIGINT <= 400 THEN 'Y'
    WHEN wfdlh.ndc IS NOT NULL THEN 'Y'
    ELSE 'N'
  END AS "Excluded Claim Indicator",
  CASE
    WHEN c.usual_and_customary_charge::BIGINT <= 400 THEN 'U&C is equal to or less than four dollars ($4.00)'
    WHEN wfdlh.ndc IS NOT NULL THEN 'Priced Drugs'
  END AS "Exclusion Reason",
  c.transaction_code AS "Reversal indicator",
  c.basis_of_reimbursement_determination_resp AS "basis of reimbursement determination"
FROM reporting.claims c
LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
-- 12 hours added to claim date of service because default init is at 00:00 AM of the date. While actual prices for that day valid from 10 AM
LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
LEFT JOIN historical_data_pbm_hippo.walmart_four_dollar_list_ndcs_history wfdlh ON wfdlh.ndc = c.product_id AND c.valid_from::timestamp >= wfdlh.valid_from::timestamp AND c.valid_from::timestamp < wfdlh.valid_to::timestamp
WHERE ph.chain_code = '229'
AND c.fill_status = 'filled'
AND c.valid_from::date >= (select period_start from date_range)
AND c.valid_from::date <= (select period_end from date_range)
AND c.valid_to::date > (select period_end from date_range)
AND c.authorization_number NOT ILIKE 'U%'
--make it match the invoice query. We can remove this when we generate invoice in python
AND (   
		((ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') OR (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != ''))
    or (
      NOT ((ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') OR (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != '')))
      OR nch.ndc IS NULL
  )    
),
reversals as (
select
  DISTINCT
  c.bin_number || '+' || c.process_control_number AS "BIN+PCN",
  c.bin_number AS "Unique Identifier",
  c.npi AS "NPI",
  '="' || c.authorization_number || '"' AS "Claim Authorization Number", --needed for excel to treat data as text not number
  c.prescription_reference_number AS "Rx#",
  c.claim_date_of_service::text AS "Service Date",
  date(c.valid_from)::text as "Fill Date",
  date(c.valid_to)::text as "Reversal Date",
  c.fill_number AS "Refill number",
  '="' || c.product_id || '"' AS "NDC", --needed for excel to treat data as text not number
  mfnm.drug_name AS "Drug Name",
  case when     
      ((ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') OR 
      (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != ''))    
      then 'nadac'
    else 'awp' end as fill_type,
  CASE 
    WHEN fill_type = 'awp' 
        AND ndcsh.multi_source_code = 'Y' 
        THEN 'generic'
    WHEN fill_type = 'nadac'        
        THEN 'generic'
    ELSE 'brand'
  END AS "Generic Indicator (Multi-Source Indicator)",
  c.quantity_dispensed AS "Total Quantity Dispensed",
  c.days_supply AS "Total days supply",
  -c.usual_and_customary_charge::BIGINT / 100::decimal(3, 0) AS "Dispensing pharmacy U&C Price",
  ISNULL(-c.percentage_sales_tax_amount_paid, 0)::BIGINT / 100::decimal(3,0) AS "Taxes Paid",
  c.total_paid_response::BIGINT / 100::decimal(3, 0) AS "Administration Fee",  
  -c.patient_pay_resp::BIGINT / 100::decimal(3, 0) AS "Patient Amount",
  -ROUND(NULLIF(nch.awp, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Average wholesale price (AWP)",
  -ROUND(COALESCE(NULLIF(nch.nadac, ''), NULLIF(nch.gpi_nadac, ''))::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Contracted NADACTR",
  -ROUND(NULLIF(nch.wac, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Wholesale acquisition cost (WAC)",  
  -c.ingredient_cost_paid_resp::BIGINT / 100::decimal(3, 0) AS "Ingredient cost paid",
  -c.dispensing_fee_paid_resp::BIGINT / 100::decimal(3, 0) AS "Dispensing fee paid",
  CASE
    WHEN c.usual_and_customary_charge::BIGINT <= 400 THEN 'Y'
    WHEN wfdlh.ndc IS NOT NULL THEN 'Y'
    ELSE 'N'
  END AS "Excluded Claim Indicator",
  CASE
    WHEN c.usual_and_customary_charge::BIGINT <= 400 THEN 'U&C is equal to or less than four dollars ($4.00)'
    WHEN wfdlh.ndc IS NOT NULL THEN 'Priced Drugs'
  END AS "Exclusion Reason",
  'B2' AS "Reversal indicator",
  c.basis_of_reimbursement_determination_resp AS "basis of reimbursement determination"
  FROM reporting.claims c
  LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
  LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
  -- 12 hours added to claim date of service because default init is at 00:00 AM of the date. While actual prices for that day valid from 10 AM
  LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
  LEFT JOIN historical_data_pbm_hippo.walmart_four_dollar_list_ndcs_history wfdlh ON wfdlh.ndc = c.product_id AND c.valid_from::timestamp >= wfdlh.valid_from::timestamp AND c.valid_from::timestamp < wfdlh.valid_to::timestamp
  WHERE ph.chain_code = '229'
  AND c.fill_status = 'filled'
  AND c.valid_from::date >= '2020-05-01'
  AND c.valid_from < (select period_start from date_range)
  AND c.valid_to::date >= (select period_start from date_range)
  AND c.valid_to::date <= (select  period_end from date_range)
)
select * from fills
union all 
select * from reversals
ORDER BY "Fill Date" DESC
