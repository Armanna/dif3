with date_range as (
select 
  dateadd('quarter',-1,date_trunc('quarter',current_date))::date AS period_start,
  (DATE_TRUNC('quarter', current_timestamp) - Interval '1 day')::date AS period_end
),
-- with date_range as (
-- select  
--     '20240401'::date as period_start,
--     '20240404'::date as period_end
-- ),
fills as (
  SELECT distinct
    c.bin_number || '+' || c.process_control_number || '+' || c.group_id AS "BIN+PCN+GROUP",
    c.bin_number AS "Unique Identifier",
    c.npi AS "NPI",
    c.authorization_number AS "Claim Authorization Number",
    c.prescription_reference_number AS "Rx#",
    c.claim_date_of_service::text AS "Service Date",
    c.valid_from as fill_date,
    '' as "Reversal Date",
    c.fill_number AS "Refill number",
    c.product_id AS "NDC",
    mfnm.drug_name AS "Drug Name",
    ph.chain_name,
    CASE
      WHEN c.claim_date_of_service::date < '2024-04-01'::date THEN -- before CostVantage
        CASE
          WHEN enh.reason = 'cvs_exempt_generics'
            THEN 'exempt generic'
          WHEN ndcsh.multi_source_code = 'Y' and ndcsh.is_otc = 'True' and fh.ndc IS NULL
            THEN 'OTC generic'
          WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC'
            THEN 'generic'
          WHEN ndcsh.multi_source_code = 'Y' and not ndcsh.is_otc = 'True' and fh.ndc IS NULL
            THEN 'generic'
          WHEN ndcsh.multi_source_code = 'Y' and fh.ndc IS NOT NULL
            THEN 'Specialty generic'    
          WHEN ndcsh.multi_source_code != 'Y' and ndcsh.is_otc = 'True' and fh.ndc IS NULL
            THEN 'OTC brand'
          ELSE 'brand'
        END
      ELSE -- CostVantage effective date is 1st if April 2024 but new data was laoded around 11:54 AM
        CASE
          WHEN enh.reason = 'cvs_exempt_generics' 
            THEN 'exempt generic'
          WHEN c.bin_number = '019901' AND ndcsh.name_type_code = 'G' 
            THEN 'generic'
          WHEN c.bin_number = '019876' AND c.claim_date_of_service::date >= '2024-07-01'::date AND ndcsh.name_type_code = 'G' 
            THEN 'generic'
          WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' OR ndcsh.multi_source_code = 'Y'
            THEN 'generic'
          ELSE 'brand'
        END
    END AS "Generic Indicator (Multi-Source Indicator)",
    c.quantity_dispensed AS "Total Quantity Dispensed",
    COALESCE(c.days_supply,30) as "Total days supply",
    c.usual_and_customary_charge::decimal(12, 4) / 100::decimal(3,0) AS "Dispensing pharmacy U&C Price",
    ISNULL(c.percentage_sales_tax_amount_paid, 0)::decimal(12, 4) / 100::decimal(3,0) AS "Taxes Paid",
    -c.total_paid_response::decimal(12, 4) / 100::decimal(3,0) AS "Administration Fee",
    c.total_paid_response::decimal(12, 4) / 100::decimal(3,0) AS "Total Amount Paid", 
    c.patient_pay_resp::decimal(12, 4) / 100::decimal(3,0) AS "Patient Amount",
    CASE
      WHEN c.basis_of_reimbursement_determination_resp = '03' THEN 'AWP'
      WHEN c.basis_of_reimbursement_determination_resp = '13' THEN 'WAC'
      WHEN c.basis_of_reimbursement_determination_resp = '07' THEN 'MAC'
      ELSE ''
    END AS price_basis,
    CASE 
      WHEN price_basis = 'AWP' THEN
        NULLIF(nch.awp,'')::decimal(16, 4) * c.quantity_dispensed::decimal(16, 4) / 100::decimal(3,0)
      WHEN price_basis = 'MAC' THEN
        NULLIF(mac.unit_cost,'')::decimal(16, 4) * c.quantity_dispensed::decimal(16,4) / 100::decimal(3,0)
      WHEN price_basis = 'WAC' THEN
        NULLIF(nch.wac,'')::decimal(16, 4) * c.quantity_dispensed::decimal(16, 4) / 100::decimal(3,0)
      ELSE NULL
    END AS total_cost,
    c.ingredient_cost_paid_resp::decimal(12, 4) / 100::decimal(3,0) AS "Ingredient cost paid",
    c.dispensing_fee_paid_resp::decimal(12, 4) / 100::decimal(3,0) AS "Dispensing fee paid",
    'N' AS "Excluded Claim Indicator",
    '' AS "Exclusion Reason",
    c.transaction_code AS "Reversal indicator"
  FROM reporting.claims c
  LEFT JOIN reporting.cardholders p on p.cardholder_id = c.n_cardholder_id
  LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  LEFT JOIN historical_data_pbm_hippo.cvs_tpdt_regular_mac_history mac on c.product_id = mac.ndc AND c.claim_date_of_service::timestamp + interval '12 hours' >= mac.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < mac.valid_to::timestamp
  LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
  LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
  LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
  LEFT JOIN historical_data_bin_019876.excluded_ndcs_history enh on enh.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= enh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < enh.valid_to::timestamp AND enh.reason = 'cvs_exempt_generics' AND c.bin_number = '019876'
  LEFT JOIN historical_data_pbm_hippo.cvs_tpdt_specialty_list_ndcs fh
      ON c.product_id = fh.ndc AND c.claim_date_of_service::timestamp + interval '12 hours' >= fh.valid_from::timestamp
      AND c.claim_date_of_service::timestamp + interval '12 hours' < fh.valid_to::timestamp
      and c.bin_number = '019901'
  WHERE ph.chain_code in ('008','039','123','177','207','673')
  and c.bin_number = '{bin}'
  -- when on main bin need to split famulus from non-famulus. CVS have asked for different bin which will make this less complicated.
  and 1 = case when p.partner = 'famulus'  and '{bin}' = '019876' and  '{partner}' = 'famulus' then 1 
                when p.partner != 'famulus' and '{bin}' = '019876' and '{partner}' = '' then 1
                -- hard code this row to deal with contract start date
                when p.partner = 'famulus' and '{bin}' = '019876' and '{partner}' = '' and c.claim_date_of_service::date < '20230121' then 1
                when '{bin}' != '019876' then 1
              else 0
          end
  AND c.fill_status = 'filled'
  and c.authorization_number not like 'U%%'
  AND c.basis_of_reimbursement_determination_resp IN ('03', '07', '13')
  AND c.valid_from :: date >= '2021-04-01' --go-live date for CVS contract
  AND c.valid_from::date >= (select period_start from date_range)
  AND c.valid_from::date <= (select period_end from date_range)
  AND c.valid_to::date > current_timestamp
  AND c.claim_date_of_service::date >= '2024-07-01'::date
  order by fill_date desc
), 
final as (
SELECT f.*,
  date(f.fill_date)::text as "Fill Date",
  f.price_basis as "Basis of reimbursement",
  cr.target_rate as "Target Effective Rate",
  (1::decimal(1, 0) -  ("Ingredient cost paid" / total_cost)) * 100::decimal(3, 0) AS "Effective Rate",
  (total_cost * ("Effective Rate" - "Target Effective Rate"::decimal(5,2)) / 100::decimal(3, 0)) as "Dollar Variance"
FROM fills f
left join (
  SELECT * 
  from reporting.chain_rates
  where 
    chain_name = 'cvs'
    and bin_number = '{bin}'
    and partner = '{partner}') cr on 
    f."Generic Indicator (Multi-Source Indicator)" = cr.drug_type
    AND f."Service Date"::date >= cr.valid_from::date AND f."Service Date"::date <= cr.valid_to::date
    AND f."Total days supply" >= cr.days_supply_from
    AND f."Total days supply" <= cr.days_supply_to
    AND f."Unique Identifier" = cr.bin_number
    AND f.price_basis = cr.price_basis
  )
  
SELECT * FROM final
