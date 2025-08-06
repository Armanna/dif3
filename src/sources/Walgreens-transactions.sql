with date_range as (
select 
  dateadd('month',-1,date_trunc('month',current_date))::date AS period_start,
  (DATE_TRUNC('month', current_timestamp) - Interval '1 second')::timestamp AS period_end
),
-- with date_range as (
-- select  
--     '20210401'::date as period_start,
--     '20210630 23:59:59'::timestamp as period_end
-- ),
fills as (
SELECT
  c.bin_number || '+' || c.process_control_number || '+' || c.group_id AS "BIN+PCN+GROUP",
  c.bin_number AS "BIN",
  c.group_id AS "Group Identification Number",
  c.npi AS "NPI",
  '="' || c.authorization_number || '"' AS "Claim Authorization Number", --needed for excel to treat data as text not number
  c.prescription_reference_number AS "Rx#",
  c.claim_date_of_service::text AS "Service Date",
  date((c.valid_from AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT')::text as "Fill Date",
  '' as "Reversal Date",
  c.fill_number AS "Refill number",
  '="' || c.product_id || '"' AS "NDC", --needed for excel to treat data as text not number
  mfnm.drug_name AS "Drug Name",
  CASE 
    WHEN ndcsh.multi_source_code = 'Y' THEN 'generic'
    ELSE 'brand'
  END AS "Generic Indicator (Multi-Source Indicator)",
  ndcsh.multi_source_code as "Occurrence Span Code",
  c.quantity_dispensed AS "Total Quantity Dispensed",
  c.days_supply AS "Total days supply",
  c.usual_and_customary_charge::BIGINT / 100::decimal(5,2) AS "Dispensing pharmacy U&C Price",
  ISNULL(c.percentage_sales_tax_amount_paid, 0)::BIGINT  / 100::decimal(5,2) AS "Taxes Paid",
  -c.total_paid_response::BIGINT / 100::decimal(5,2) AS "Administration Fee",
  c.total_paid_response::BIGINT / 100::decimal(5,2) AS "Total Amount Paid", 
  c.patient_pay_resp::BIGINT / 100::decimal(5,2) AS "Patient Amount",
  ROUND(nch.awp::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Average wholesale price (AWP)",
  c.ingredient_cost_paid_resp::BIGINT / 100::decimal(5,2) AS "Ingredient cost paid",
  c.dispensing_fee_paid_resp::BIGINT / 100::decimal(5,2) AS "Dispensing fee paid",
  'N' AS "Excluded Claim Indicator",
  '' AS "Exclusion Reason",
  c.transaction_code AS "Reversal indicator",
  c.basis_of_reimbursement_determination_resp AS "basis of reimbursement determination"
FROM reporting.claims c
LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
-- 12 hours added to claim date of service because default init is at 00:00 AM of the date. While actual prices for that day valid from 10 AM
LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
WHERE ph.chain_code in ('226','A10','A13')
AND c.fill_status = 'filled'
and c.authorization_number not like 'U%'
AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' >= '2021-05-01' --go-live date for Walgreens contract
AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' >= (select period_start from date_range)
AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' <= (select period_end from date_range)
AND (c.valid_to::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' > (select period_end from date_range)
)
select * from fills
ORDER BY "Fill Date" DESC
