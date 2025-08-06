with date_range as (
select 
dateadd('quarter',-1,date_trunc('quarter',current_date))::date AS period_start,
(DATE_TRUNC('quarter', current_timestamp) - Interval '1 day')::date AS period_end
)
-- with date_range as (
-- select 
--       '20240101'::date as period_start,
--       '20240131'::date as period_end
-- )
SELECT
  c.bin_number || '+' || c.process_control_number AS "BIN+PCN",
  c.bin_number AS "Unique Identifier",
  c.npi::text AS "NPI",
  c.authorization_number AS "Claim Authorization Number",
  c.prescription_reference_number AS "Rx#",
  c.claim_date_of_service::text AS "Service Date",
  c.fill_number AS "Refill number",
  --c.response_time::date AS "Date of Fill", --use ETC date and claim valid time
  c.product_id AS "NDC",
  mfnm.drug_name AS "Drug Name",
  CASE 
    WHEN ndcsh.nadac_is_generic = 'True'
        THEN 'generic'
    WHEN ndcsh.nadac_is_generic = '' and ndcsh.multi_source_code = 'Y'
        THEN 'generic'
    ELSE 'brand'
  END AS "Generic Indicator (Multi-Source Indicator)",
  CASE 
    WHEN "Generic Indicator (Multi-Source Indicator)" = 'generic' AND (nch.gpi_nadac != '' OR nch.nadac != '')
      THEN 'nadac'
  ELSE 'awp' END AS fill_type,
  c.quantity_dispensed AS "Total Quantity Dispensed",
  c.days_supply AS "Total days supply",
  c.usual_and_customary_charge::BIGINT / 100::decimal(3, 0) AS "Dispensing pharmacy U&C Price",
  ISNULL(c.percentage_sales_tax_amount_paid, 0)::BIGINT  / 100::decimal(3, 0) AS "Taxes Paid",
  c.total_paid_response::BIGINT / 100::decimal(3, 0) AS "Administration Fee",
  c.patient_pay_resp::BIGINT / 100::decimal(3, 0) AS "Patient Amount",
  ROUND(NULLIF(nch.awp, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4), 0)::BIGINT / 100::decimal(3, 0) AS "Average wholesale price (AWP)",
  case when fill_type = 'nadac'
    then 
      ROUND(COALESCE(NULLIF(nch.nadac, ''), NULLIF(nch.gpi_nadac, ''))::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4), 0)::BIGINT / 100::decimal(3, 0)
    else 0 end AS contracted_nadactr,
  --ROUND(COALESCE(nh.nadac_per_unit::float * 100, gnh.unit_cost::float) * c.quantity_dispensed::float, 0)::BIGINT / 100::decimal(3, 0) AS "Contracted NADACTR",
  c.ingredient_cost_paid_resp::BIGINT / 100::decimal(3, 0) AS "Ingredient cost paid",
  c.dispensing_fee_paid_resp::BIGINT / 100::decimal(3, 0) AS "Dispensing fee paid",
  CASE
    WHEN c.usual_and_customary_charge::BIGINT <= 400 THEN 'Y'
    WHEN wfdlh.ndc IS NOT NULL THEN 'Y'
    WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'Y'
    ELSE 'N'
  END AS "Excluded Claim Indicator",
  CASE
    WHEN c.usual_and_customary_charge::BIGINT <= 400 THEN 'U&C is equal to or less than four dollars ($4.00)'
    WHEN wfdlh.ndc IS NOT NULL THEN 'Priced Drugs'
    WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'U&C transaction'
    ELSE NULL
  END AS "Exclusion Reason",
  c.transaction_code AS "Reversal indicator"
FROM reporting.claims c
LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  --ndc_costs_history data starts on 1 June 2021 so needs backfill for this to revert to prior version
LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND
    case when c.claim_date_of_service::timestamp < '20210601' then '20210601'::timestamp else c.claim_date_of_service::timestamp end
    + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
LEFT JOIN historical_data_pbm_hippo.walmart_four_dollar_list_ndcs_history wfdlh ON wfdlh.ndc = c.product_id AND c.valid_from::timestamp >= wfdlh.valid_from::timestamp AND c.valid_from::timestamp < wfdlh.valid_to::timestamp
--can not use chain code as not back-populated in pharmacy history
WHERE ph.chain_name = 'walmart'
--WHERE ph.chain_code = '229'
AND c.fill_status = 'filled'
AND c.valid_from::date >= (select period_start from date_range)
AND c.valid_to::date > (select period_end from date_range)
AND c.valid_from::date <= (select period_end from date_range)
AND c.claim_date_of_service::date < '20240903'::date
ORDER BY c.valid_from DESC
