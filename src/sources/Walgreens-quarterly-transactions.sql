with date_range as (
select
  dateadd('quarter',-1,date_trunc('quarter',current_date))::date AS period_start,
  (DATE_TRUNC('quarter', current_timestamp) - Interval '1 second')::timestamp AS period_end
),
-- with date_range as (
-- select
--   dateadd('week',-4,date_trunc('week',current_date))::date AS period_start,
--   dateadd('week',-2,date_trunc('week',current_date))::date AS period_end
--   -- (DATE_TRUNC('week', current_timestamp) - Interval '1 second')::timestamp AS period_end
-- ),
chain_rates as (
SELECT *,
    CASE
      WHEN days_supply_to = 83 THEN '84-'
      WHEN days_supply_to >= 84 THEN '84+'
      ELSE 'error'
    END AS ds_class
  FROM reporting.chain_rates
  WHERE 1=1
    AND chain_name = 'walgreens'
),
claims as (
SELECT
  c.bin_number || '+' || c.process_control_number || '+' || c.group_id AS "BIN+PCN+GROUP",
  c.bin_number AS "BIN",
  c.group_id AS "Group Identification Number",
  c.npi AS "NPI",
  c.authorization_number AS "Claim Authorization Number",
  c.prescription_reference_number AS "Rx#",
  c.claim_date_of_service::text AS "Service Date",
  ph.state_abbreviation,
  date((c.valid_from AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT')::text as "Fill Date",
  c.fill_number AS "Refill number",
  c.product_id AS "NDC",
  mfnm.drug_name AS "Drug Name",
  CASE
    WHEN ndcsh.multi_source_code = 'Y' THEN 'generic'
    ELSE 'brand'
  END AS "Generic Indicator (Multi-Source Indicator)",
  ndcsh.multi_source_code as "Occurrence Span Code",
  CASE
    WHEN c.claim_date_of_service::timestamp < '2025-06-12'::timestamp THEN 'AWP'
    WHEN c.basis_of_reimbursement_determination_resp = '03' THEN 'AWP'
    WHEN c.basis_of_reimbursement_determination_resp = '13' THEN 'WAC'
    WHEN c.basis_of_reimbursement_determination_resp = '07' THEN 'MAC'
    ELSE basis_of_reimbursement_determination_resp
  END AS "Price Basis",
  CASE
    WHEN c.days_supply <= 83 THEN '84-'
    WHEN c.days_supply >= 84 THEN '84+'
    ELSE 'error'
  END AS ds_class,
  CASE
    WHEN c.claim_date_of_service::timestamp < '2025-06-12'::timestamp AND c.bin_number='019876' THEN 'hippo_walgreens_2nd_amendment'
    WHEN c.claim_date_of_service::timestamp < '2025-06-12'::timestamp AND c.bin_number='026465' THEN 'hippo_wags_finder_original'
    WHEN c.bin_number='026465' THEN 'hippo_wags_finder'
    WHEN c.network_reimbursement_id in ('2274') THEN 'hippo_walgreens'
    WHEN c.network_reimbursement_id in ('INTCAP', 'INTRXP', 'INTRXIBO', '019876') THEN 'hippo_walgreens_integrated'
    WHEN c.network_reimbursement_id in ('LNGRX') THEN 'hippo_walgreens_legacy_rental'
    ELSE 'hippo_walgreens'
  END as contract_name,
  c.quantity_dispensed AS "Total Quantity Dispensed",
  c.days_supply AS "Total days supply",
  c.usual_and_customary_charge::decimal(12, 4) / 100::decimal(3,0) AS "Dispensing pharmacy U&C Price",
  ISNULL(c.percentage_sales_tax_amount_paid, 0)::decimal(12, 4) / 100::decimal(5,2) AS "Taxes Paid",
  -c.total_paid_response::decimal(12, 4) / 100::decimal(3,0) AS "Administration Fee",
  c.total_paid_response::decimal(12, 4) / 100::decimal(3,0) AS "Total Amount Paid",
  c.patient_pay_resp::decimal(12, 4) / 100::decimal(3,0) AS "Patient Amount",
  (nch.awp::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4)) / 100::decimal(3, 0) AS "Average wholesale price (AWP)",
  (mac.unit_cost::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4)) / 100::decimal(3, 0) AS "Maximum Allowable Cost (MAC)",
  c.ingredient_cost_paid_resp::decimal(12, 4) / 100::decimal(3,0) AS "Ingredient cost paid",
  c.dispensing_fee_paid_resp::decimal(12, 4) / 100::decimal(3,0) AS "Dispensing fee paid",
  'N' AS "Excluded Claim Indicator",
  '' AS "Exclusion Reason",
  c.transaction_code AS "Reversal indicator"
FROM reporting.claims c
LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
LEFT JOIN historical_data_bin_019876.mac_v2_history mac ON mac.ndc = c.product_id AND mac_source='walgreens_pac' AND c.claim_date_of_service::timestamp + interval '12 hours' >= mac.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < mac.valid_to::timestamp
WHERE 1=1
  AND ph.chain_code in ('226','A10','A13')
  AND c.basis_of_reimbursement_determination_resp!='04'
  AND (c.valid_from::date AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' >= '2021-05-01' --go-live date for Walgreens contract
  AND c.fill_status = 'filled'
  AND c.authorization_number not like 'U%'
  AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' >= (select period_start from date_range)
  AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' <= (select period_end from date_range)
  AND (c.valid_to::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' > (select period_end from date_range)
),
fills as (
SELECT
  "BIN+PCN+GROUP",
  "BIN",
  "Group Identification Number",
  "NPI",
  "Claim Authorization Number",
  "Rx#",
  "Service Date",
  "Fill Date",
  "Refill number",
  "NDC",
  "Drug Name",
  "Generic Indicator (Multi-Source Indicator)",
  "Occurrence Span Code",
  "Total Quantity Dispensed",
  "Total days supply",
  "Dispensing pharmacy U&C Price",
  "Taxes Paid",
  "Administration Fee",
  "Total Amount Paid",
  "Patient Amount",
  "Price Basis",
  "Average wholesale price (AWP)",
  "Maximum Allowable Cost (MAC)",
  CASE
    WHEN "Price Basis" = 'AWP' THEN "Average wholesale price (AWP)"
    WHEN "Price Basis" = 'MAC' THEN "Maximum Allowable Cost (MAC)"
    ELSE 0::decimal(13, 4)
  END AS "Drug Cost",
  "Ingredient cost paid",
  "Dispensing fee paid",
  "Excluded Claim Indicator",
  "Exclusion Reason",
  "Reversal indicator",
  (1::decimal(1, 0) -  ("Ingredient cost paid" / "Drug Cost")) * 100::decimal(3, 0) as "Actual IC Rate",
  isnull(cr.target_rate, default_cr.target_rate) as "Target IC Rate",
  isnull(cr.dfer_target_rate, default_cr.dfer_target_rate) as "Target DFER",
  "Actual IC Rate" - "Target IC Rate" as "IC Rate Variance",
  ("Drug Cost" * "IC Rate Variance" / 100::decimal(3, 0)) as "IC Rate Dollar Variance",
  "Target DFER" - "Dispensing fee paid" as "DFER Dollar Variance",
  -- Note: DFER variance calculated as "target - actual" for clarity.
  -- IC variance uses "actual - target" because IC is defined as `1 - (cost_paid / drug_cost)`,
  -- so the calculation naturally inverses the relationship.
  "IC Rate Dollar Variance" + "DFER Dollar Variance" as "Total Dollar Variance"
FROM claims c
LEFT JOIN chain_rates cr -- this JOIN suppose to join specific rates for each state
    ON "Generic Indicator (Multi-Source Indicator)"= cr.drug_type
    AND c.contract_name = cr.contract_name
    AND "BIN" = cr.bin_number
    AND cr.state_abbreviation = c.state_abbreviation
    AND c."Price Basis"=cr.price_basis
    AND ((c."Price Basis" = 'MAC' AND c.ds_class = cr.ds_class) or (c."Price Basis" = 'AWP')) -- Only mac has per DS rates
    AND "Service Date"::timestamp >= cr.valid_from::timestamp
    AND "Service Date"::timestamp < cr.valid_to::timestamp
LEFT JOIN chain_rates default_cr -- this JOIN suppose to join specific rates for the rest of the states
    ON "Generic Indicator (Multi-Source Indicator)" = default_cr.drug_type
    AND "BIN" = default_cr.bin_number
    AND default_cr.state_abbreviation = ''
    AND c.contract_name = default_cr.contract_name
    AND c."Price Basis"=default_cr.price_basis
    AND ((c."Price Basis" = 'MAC' AND c.ds_class = default_cr.ds_class) or (c."Price Basis" = 'AWP')) -- Only mac has per DS rates
    AND "Service Date"::timestamp >= default_cr.valid_from::timestamp
    AND "Service Date"::timestamp < default_cr.valid_to::timestamp
)
--contract says
--for all claims for Covered Drugs dispensed during that calendar quarter, excluding any reversed claims.
select * from fills
ORDER BY "Fill Date" DESC
