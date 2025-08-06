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
  ph.ncpdp as "NCPDP",
  c.npi AS "NPI",
  c.bin_number,
  c.prescription_reference_number AS "Prescription Number",
  c.claim_date_of_service,
  ph.state_abbreviation,
  date((c.valid_from AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT')::text as "Date Filled",
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
  CASE
    WHEN c.claim_date_of_service::timestamp < '2025-06-12'::timestamp AND c.bin_number='019876' THEN 'hippo_walgreens_2nd_amendment'
    WHEN c.claim_date_of_service::timestamp < '2025-06-12'::timestamp AND c.bin_number='026465' THEN 'hippo_wags_finder_original'
    WHEN c.bin_number='026465' THEN 'hippo_wags_finder'
    WHEN c.network_reimbursement_id in ('2274') THEN 'hippo_walgreens'
    WHEN c.network_reimbursement_id in ('INTCAP', 'INTRXP', 'INTRXIBO', '019876') THEN 'hippo_walgreens_integrated'
    WHEN c.network_reimbursement_id in ('LNGRX') THEN 'hippo_walgreens_legacy_rental'
    ELSE 'hippo_walgreens'
  END as contract_name,
  CASE
    WHEN ndcsh.multi_source_code = 'Y' THEN 'generic'
    ELSE 'brand'
  END AS "Prescription Type",
  m.store_number as "Store Number",
  c.patient_pay_resp::decimal(12, 4) / 100::decimal(3,0) as "PARTICIPANT pay amount",
  -c.total_paid_response::decimal(12, 4) / 100::decimal(3,0) AS "Administration Fee"
FROM reporting.claims c
LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
left join data_q.mas m on m.npi = c.npi
where ph.chain_code in ('226','A10','A13')
AND c.fill_status = 'filled'
and c.authorization_number not like 'U%'
AND c.basis_of_reimbursement_determination_resp!='04'
AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' >= '2021-05-01' --go-live date for Walgreens contract
AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' >= (select period_start from date_range)
AND (c.valid_from::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' <= (select period_end from date_range)
AND (c.valid_to::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'CST6CDT' > (select period_end from date_range)
),
fills as (
SELECT
  "NCPDP",
  "NPI",
  "Prescription Number",
  "Date Filled",
  "Prescription Type",
  c.basis_of_reimbursement_source as "Price Basis",
  "Store Number",
  isnull(cr.target_rate, default_cr.target_rate) as "Contracted Rate",
  "PARTICIPANT pay amount",
  "Administration Fee"
FROM claims c
LEFT JOIN chain_rates cr -- this JOIN suppose to join specific rates for each state
    ON "Prescription Type" = cr.drug_type
    AND c.contract_name = cr.contract_name
    AND c.bin_number = cr.bin_number
    AND cr.state_abbreviation = c.state_abbreviation
    AND c.basis_of_reimbursement_source=cr.price_basis
    AND ((c.basis_of_reimbursement_source = 'MAC' AND c.ds_class = cr.ds_class) or (c.basis_of_reimbursement_source = 'AWP')) -- Only mac has per DS rates
    AND c.claim_date_of_service::timestamp >= cr.valid_from::timestamp
    AND c.claim_date_of_service::timestamp < cr.valid_to::timestamp
LEFT JOIN chain_rates default_cr -- this JOIN suppose to join specific rates for the rest of the states
    ON "Prescription Type" = default_cr.drug_type
    AND c.bin_number = default_cr.bin_number
    AND default_cr.state_abbreviation = ''
    AND c.contract_name = default_cr.contract_name
    AND c.basis_of_reimbursement_source=default_cr.price_basis
    AND ((c.basis_of_reimbursement_source = 'MAC' AND c.ds_class = default_cr.ds_class) or (c.basis_of_reimbursement_source = 'AWP')) -- Only mac has per DS rates
    AND c.claim_date_of_service::timestamp >= default_cr.valid_from::timestamp
    AND c.claim_date_of_service::timestamp < default_cr.valid_to::timestamp
)
--contract says
--for all claims for Covered Drugs dispensed during that calendar quarter, excluding any reversed claims.
select

  "NCPDP",
  "NPI",
  "Prescription Number",
  "Date Filled",
  "Prescription Type",
  "Price Basis",
  "Store Number",
  "Contracted Rate",
  "PARTICIPANT pay amount",
  "Administration Fee"
from fills
ORDER BY "Date Filled" DESC
