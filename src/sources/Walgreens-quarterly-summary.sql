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
    (select period_start from date_range) as period_start,
    (select period_end from date_range) as period_end,
    EXTRACT('year' FROM (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT') AS year,
    CASE
      WHEN ndcsh.multi_source_code = 'Y' THEN 'generic'
      ELSE 'brand'
    END AS drug_type,
    CASE
      WHEN c.claim_date_of_service::timestamp < '2025-06-12'::timestamp THEN 'AWP'
      WHEN c.basis_of_reimbursement_determination_resp = '03' THEN 'AWP'
      WHEN c.basis_of_reimbursement_determination_resp = '13' THEN 'WAC'
      WHEN c.basis_of_reimbursement_determination_resp = '07' THEN 'MAC'
      ELSE basis_of_reimbursement_determination_resp
    END AS basis_of_reimbursement_source,
    CASE
      WHEN c.days_supply <= 83 THEN '84-'
      WHEN c.days_supply >= 84 THEN '84+'
      ELSE 'error'
    END AS ds_class,
    c.bin_number,
    ph.state_abbreviation,
    ph.chain_code,
    c.valid_from,
    c.valid_to,
    c.ingredient_cost_paid_resp,
    c.dispensing_fee_paid_resp,
    c.total_paid_response,
    nch.awp::decimal(13, 4) as awp,
    mac.unit_cost::decimal(13, 4) as mac,
    c.quantity_dispensed::decimal(10, 4) as quantity_dispensed,
    c.fill_status,
    c.authorization_number,
    c.claim_date_of_service,
    CASE
      WHEN c.claim_date_of_service::timestamp < '2025-06-12'::timestamp AND c.bin_number='019876' THEN 'hippo_walgreens_2nd_amendment'
      WHEN c.claim_date_of_service::timestamp < '2025-06-12'::timestamp AND c.bin_number='026465' THEN 'hippo_wags_finder_original'
      WHEN c.bin_number='026465' THEN 'hippo_wags_finder'
      WHEN c.network_reimbursement_id in ('2274') THEN 'hippo_walgreens'
      WHEN c.network_reimbursement_id in ('INTCAP', 'INTRXP', 'INTRXIBO', '019876') THEN 'hippo_walgreens_integrated'
      WHEN c.network_reimbursement_id in ('LNGRX') THEN 'hippo_walgreens_legacy_rental'
      ELSE 'hippo_walgreens'
    END as contract_name
  FROM
    reporting.claims c
    LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.mac_v2_history mac ON mac.ndc = c.product_id AND mac_source='walgreens_pac' AND c.claim_date_of_service::timestamp + interval '12 hours' >= mac.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < mac.valid_to::timestamp
  WHERE 1=1
    AND ph.chain_code in ('226','A10','A13')
    AND c.basis_of_reimbursement_determination_resp!='04'
    AND (c.valid_from::date AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' >= '2021-05-01' --go-live date for Walgreens contract
    AND c.fill_status = 'filled'
    and c.authorization_number not like 'U%'
    AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' >= (select period_start from date_range)
    AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' <= (select period_end from date_range)
    AND (c.valid_to::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' > (select period_end from date_range)
  -- LIMIT 1000
),
fills AS (
  SELECT
    period_start,
    period_end,
    year,
    basis_of_reimbursement_source,
    c.drug_type,
    c.bin_number,
    c.contract_name,
    isnull(cr.target_rate, default_cr.target_rate) as target_rate,
    isnull(cr.dfer_target_rate, default_cr.dfer_target_rate) as dfer_target_rate,
    SUM(c.ingredient_cost_paid_resp) AS total_ingredient_cost_paid,
    SUM(c.total_paid_response) AS total_administration_fee,
    CASE
      when basis_of_reimbursement_source='AWP' THEN SUM(c.awp * c.quantity_dispensed)
      when basis_of_reimbursement_source='MAC' THEN SUM(c.mac * c.quantity_dispensed)
    END as total_drug_cost,
    SUM(c.dispensing_fee_paid_resp) as TotalDFPaid,
    SUM(isnull(cr.dfer_target_rate, default_cr.dfer_target_rate)) as TotalDFContracted,
    COUNT(*) AS claim_count
  FROM
    claims c
    LEFT JOIN chain_rates cr -- this JOIN suppose to join specific rates for each state
        ON c.drug_type = cr.drug_type
        AND c.contract_name = cr.contract_name
        AND c.bin_number = cr.bin_number
        AND cr.state_abbreviation = c.state_abbreviation
        AND c.basis_of_reimbursement_source=cr.price_basis
        AND ((c.basis_of_reimbursement_source = 'MAC' AND c.ds_class = cr.ds_class) or (c.basis_of_reimbursement_source = 'AWP')) -- Only mac has per DS rates
        AND c.claim_date_of_service::timestamp >= cr.valid_from::timestamp
        AND c.claim_date_of_service::timestamp < cr.valid_to::timestamp
    LEFT JOIN chain_rates default_cr -- this JOIN suppose to join specific rates for the rest of the states
        ON c.drug_type = default_cr.drug_type
        AND c.bin_number = default_cr.bin_number
        AND default_cr.state_abbreviation = ''
        AND c.contract_name = default_cr.contract_name
        AND c.basis_of_reimbursement_source=default_cr.price_basis
        AND ((c.basis_of_reimbursement_source = 'MAC' AND c.ds_class = default_cr.ds_class) or (c.basis_of_reimbursement_source = 'AWP')) -- Only mac has per DS rates
        AND c.claim_date_of_service::timestamp >= default_cr.valid_from::timestamp
        AND c.claim_date_of_service::timestamp < default_cr.valid_to::timestamp
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
per_contract_report as (
SELECT
  to_char(f.period_start::date, 'MM/DD/YYYY') as period_start,
  to_char(f.period_end::date,'MM/DD/YYYY') AS period_end,
  f.drug_type,
  -- f.ds_class,
  f.target_rate,
  f.dfer_target_rate,
  f.basis_of_reimbursement_source AS basis_of_reimbursement_source,
  f.contract_name,
  isnull(f.claim_count::BIGINT,0) AS claim_count,
  isnull(f.total_ingredient_cost_paid, 0) / 100::decimal(3,0) AS total_ingredient_cost_paid,
  -isnull(f.total_administration_fee ,0) / 100::decimal(3,0) AS total_administration_fee,
  isnull(f.total_drug_cost ,0) / 100::decimal(3, 0)  AS total_drug_cost,
  ((1 - (total_ingredient_cost_paid/ total_drug_cost)) * 100::decimal(3,0)) AS actual_effective_rate, --seems decimals don't round numbers to dp, they truncate them
  actual_effective_rate - target_rate as effective_rate_variance,
  total_drug_cost * (actual_effective_rate - target_rate::decimal(7,5)) / 100::decimal(3, 0) as effective_rate_dollar_variance,
  isnull(f.TotalDFPaid, 0) / 100::decimal(3,0) AS total_dispensing_fee_paid,
  isnull(f.TotalDFContracted, 0) AS total_dispensing_fee_contracted,
  (total_dispensing_fee_paid / claim_count::decimal(14,0)) AS actual_dfer_rate, --seems decimals don't round numbers to dp, they truncate them
  dfer_target_rate - actual_dfer_rate  as dfer_variance,
  -- Note: DFER variance calculated as "target - actual" for clarity.
  -- IC variance uses "actual - target" because IC is defined as `1 - (cost_paid / drug_cost)`,
  -- so the calculation naturally inverses the relationship.
  dfer_variance * claim_count as dfer_dollar_variance
  -- "Total Drug Cost" * ("Actual Effective Rate" - "Target E?ffective Rate"::decimal(7,5)) / 100::decimal(3, 0) as "Effective Rate Dollar Variance",
FROM
  fills f
)
SELECT
  period_start,
  period_end,
  drug_type as "Drug Type",
  basis_of_reimbursement_source as "Price Basis",
  target_rate as "Target IC Rate",
  dfer_target_rate as "Target DFER",
  sum(claim_count) as "claim count",
  sum(total_ingredient_cost_paid) as "Total Ingredient Cost Paid",
  sum(total_administration_fee) as "Total Administration Fee",
  sum(total_drug_cost) as "Total Drug Cost",
  ((1 - ("Total Ingredient Cost Paid"::decimal(18,4) / "Total Drug Cost"::decimal(18,4))) * 100::decimal(3,0)) AS "Actual IC Rate", --seems decimals don't round numbers to dp, they truncate them
  "Actual IC Rate" - "Target IC Rate" as "IC Rate Variance",
  "Total Drug Cost" * "IC Rate Variance" / 100::decimal(3, 0) as "IC Rate Dollar Variance",
  sum(total_dispensing_fee_paid) as "Total Dispensing Fee Paid",
  sum(total_dispensing_fee_contracted) as "Total Dispensing Fee Contracted",
  ("Total Dispensing Fee Paid"::decimal(18,4) / "claim count") AS "Actual DFER Rate", --seems decimals don't round numbers to dp, they truncate them
  "Target DFER" - "Actual DFER Rate"  as "DFER Variance",
  -- Note: DFER variance calculated as "target - actual" for clarity.
  -- IC variance uses "actual - target" because IC is defined as `1 - (cost_paid / drug_cost)`,
  -- so the calculation naturally inverses the relationship.
  "DFER Variance" * "claim count" as "DFER Dollar Variance",
  "IC Rate Dollar Variance" + "DFER Dollar Variance" as "Total Dollar Variance"
FROM
  per_contract_report p
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 3, 4, 5, 6
