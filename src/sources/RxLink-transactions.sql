-- with date_range as (
-- select 
--   dateadd('month',-1,date_trunc('month',current_date))::date AS period_start,
--   (DATE_TRUNC('month', current_timestamp) - Interval '1 day')::date AS period_end
-- ),
with date_range as (
select  
  '{period_start}'::date AS period_start,
  '{period_end}'::date AS period_end
),
fills as (
SELECT
  c.bin_number as "BIN",
  c.process_control_number as "PCN",
  c.group_id AS "GROUP",
  c.npi AS "NPI",
  '="' || c.authorization_number || '"' AS "Claim Authorization Number", --needed for excel to treat data as text not number
  c.prescription_reference_number AS "Rx#",
  c.claim_date_of_service::text AS "Service Date",
  date(c.valid_from)::text as "Fill Date",
  '' as "Reversal Date",
  c.fill_number AS "Refill number",
  '="' || c.product_id || '"' AS "NDC", --needed for excel to treat data as text not number
  mfnm.drug_name AS "Drug Name",
  c.quantity_dispensed AS "Total Quantity Dispensed",
  c.days_supply AS "Total days supply",
  c.usual_and_customary_charge::BIGINT / 100::decimal(5,2) AS "Dispensing pharmacy U&C Price",
  ISNULL(c.percentage_sales_tax_amount_paid, 0)::BIGINT / 100::decimal(5,2) AS "Taxes Paid",
  -c.total_paid_response::BIGINT / 100::decimal(5,2) AS "Administration Fee",
  c.total_paid_response::BIGINT / 100::decimal(5,2) AS "Total Amount Paid", 
  c.patient_pay_resp::BIGINT / 100::decimal(5,2) AS "Patient Amount",
  ROUND(nch.awp::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Average wholesale price (AWP)",
  c.ingredient_cost_paid_resp::BIGINT / 100::decimal(5,2) AS "Ingredient cost paid",
  c.dispensing_fee_paid_resp::BIGINT / 100::decimal(5,2) AS "Dispensing fee paid",
  c.transaction_code AS "Reversal indicator",
  c.basis_of_reimbursement_determination_resp AS "basis of reimbursement determination",
  CASE 
    WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'N'
    WHEN ph.is_in_network != 'True' THEN 'N'
    ELSE 'Y'
  END AS "Profitability flag",
  CASE 
    WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'UNC'
    WHEN ph.is_in_network != 'True' THEN 'NOT IN THE NETWORK'
    ELSE ''
  END AS "Exclusion reason"
FROM reporting.claims c
join reporting.cardholders p on c.cardholder_id = p.cardholder_id
LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
WHERE 
    c.fill_status = 'filled'
    AND p.partner = 'rxlink'
    AND c.valid_from::date >= (select period_start from date_range)
    AND c.valid_from::date <= (select period_end from date_range)
    AND c.valid_to::date > (select period_end from date_range)
    AND c.claim_date_of_service::date >= '2023-01-02'::date -- contract Effecive Date
),
reversals as (
SELECT 
  c.bin_number as "BIN",
  c.process_control_number as "PCN",
  c.group_id AS "GROUP",
  c.npi AS "NPI",
  '="' || c.authorization_number || '"' AS "Claim Authorization Number", --needed for excel to treat data as text not number
  c.prescription_reference_number AS "Rx#",
  c.claim_date_of_service::text AS "Service Date",
  date(c.valid_from)::text as "Fill Date",
  date(c.valid_to)::text as "Reversal Date",
  c.fill_number AS "Refill number",
  '="' || c.product_id || '"' AS "NDC", --needed for excel to treat data as text not number
  mfnm.drug_name AS "Drug Name",
  c.quantity_dispensed AS "Total Quantity Dispensed",
  c.days_supply AS "Total days supply",
  -c.usual_and_customary_charge::BIGINT / 100::decimal(5,2) AS "Dispensing pharmacy U&C Price",
  ISNULL(-c.percentage_sales_tax_amount_paid, 0)::BIGINT / 100::decimal(5,2) AS "Taxes Paid",
  c.total_paid_response::BIGINT / 100::decimal(5,2) AS "Administration Fee",
  -c.total_paid_response::BIGINT / 100::decimal(5,2) AS "Total Amount Paid", 
  -c.patient_pay_resp::BIGINT / 100::decimal(5,2) AS "Patient Amount",
  ROUND(nch.awp::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Average wholesale price (AWP)",
  -c.ingredient_cost_paid_resp::BIGINT / 100::decimal(5,2) AS "Ingredient cost paid",
  -c.dispensing_fee_paid_resp::BIGINT / 100::decimal(5,2) AS "Dispensing fee paid",
  'B2' AS "Reversal indicator",
  c.basis_of_reimbursement_determination_resp AS "basis of reimbursement determination",
  CASE 
    WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'N'
    WHEN ph.is_in_network != 'True' THEN 'N'
    ELSE 'Y'
  END AS "Profitability flag",
  CASE 
    WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'UNC'
    WHEN ph.is_in_network != 'True' THEN 'NOT IN THE NETWORK'
    ELSE ''
  END AS "Exclusion reason"
FROM reporting.claims c
join reporting.cardholders p on c.cardholder_id = p.cardholder_id
LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
WHERE 
    c.fill_status = 'filled'
    AND p.partner = 'rxlink'
    and c.valid_from < (select period_start from date_range)
    AND c.valid_to::date >= (select period_start from date_range)
    AND c.valid_to::date <= (select  period_end from date_range)
    AND c.claim_date_of_service::date >= '2023-01-02'::date -- contract Effecive Date
)
select * from fills
union all 
select * from reversals
ORDER BY "Fill Date" DESC
