with date_range as (
select 
  dateadd('quarter',-1,date_trunc('quarter',current_date))::date AS period_start,
  (DATE_TRUNC('quarter', current_timestamp) - Interval '1 day')::date AS period_end
  --date_trunc('year', dateadd(year, -1, current_date))::date AS period_start,
  --(DATE_TRUNC('year', current_timestamp) - Interval '1 day')::date AS period_end
),
-- with date_range as (
-- select  
--     '20240401'::date as period_start,
--     '20240404'::date as period_end
-- ),
fills AS (
  SELECT
    c.claim_date_of_service::date as fill_date,
    EXTRACT('year' FROM c.valid_from) AS year,
    c.valid_from::timestamp,
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
      ELSE -- CostVantage effective date is 1 April 2024
        CASE
          WHEN enh.reason = 'cvs_exempt_generics' 
            THEN 'exempt generic'
          WHEN c.bin_number = '019901' AND ndcsh.name_type_code = 'G' 
            THEN 'generic'
          WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' OR ndcsh.multi_source_code = 'Y'
            THEN 'generic'
          ELSE 'brand'
        END
    END AS drug_type,
    bin_number,
    ingredient_cost_paid_resp,
    total_paid_response,
    dispensing_fee_paid,
    COALESCE(days_supply,30) as days_supply,
    CASE
      WHEN c.basis_of_reimbursement_determination_resp = '03' THEN 'AWP'
      WHEN c.basis_of_reimbursement_determination_resp = '13' THEN 'WAC'
      WHEN c.basis_of_reimbursement_determination_resp = '07' THEN 'MAC'
      ELSE ''
    END AS price_basis,
    CASE 
      WHEN price_basis = 'AWP' THEN
        NULLIF(ncv2h.awp,'')::decimal(16, 4) * c.quantity_dispensed::decimal(16, 4)
      WHEN price_basis = 'MAC' THEN
        NULLIF(mac.unit_cost,'')::decimal(16, 4) * c.quantity_dispensed::decimal(16,4)
      WHEN price_basis = 'WAC' THEN
        NULLIF(ncv2h.wac,'')::decimal(16, 4) * c.quantity_dispensed::decimal(16, 4)
      ELSE NULL
    END AS full_cost
  FROM
    reporting.claims c
    LEFT JOIN reporting.cardholders p on p.cardholder_id = c.n_cardholder_id
    LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from :: timestamp >= ph.valid_from :: timestamp AND c.valid_from :: timestamp < ph.valid_to :: timestamp
    LEFT JOIN historical_data_pbm_hippo.cvs_tpdt_regular_mac_history mac on c.product_id = mac.ndc AND c.claim_date_of_service::timestamp + interval '12 hours' >= mac.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < mac.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history ncv2h ON ncv2h.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ncv2h.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ncv2h.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.excluded_ndcs_history enh on enh.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= enh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < enh.valid_to::timestamp AND enh.reason = 'cvs_exempt_generics' AND c.bin_number = '019876'
    LEFT JOIN historical_data_pbm_hippo.cvs_tpdt_specialty_list_ndcs fh
        ON c.product_id = fh.ndc AND c.claim_date_of_service::timestamp + interval '12 hours' >= fh.valid_from::timestamp
        AND c.claim_date_of_service::timestamp + interval '12 hours' < fh.valid_to::timestamp
        and c.bin_number = '019901'
  WHERE
    ph.chain_code in ('008','039','123','177','207','673')
    and c.bin_number = '019901'    
    -- when on main bin need to split webmd from non-webmd. CVS have asked for different bin which will make this less complicated.
    and 1 = case when p.partner = 'webmd'  and '019901' = '019876' and '' = 'webmd' then 1 
                 when p.partner != 'webmd' and '019901' = '019876' and '' = '' then 1
                 -- hard code this row to deal with contract start date
                 when p.partner = 'webmd' and '019901' = '019876' and '' = '' and c.claim_date_of_service::date < '20230121' then 1
                 when '019901' != '019876' then 1
                else 0
            end
    AND c.valid_from::date >= '2021-04-01' --go-live date for CVS contract
    AND c.fill_status = 'filled'
    and c.authorization_number not like 'U%%'
    AND c.valid_from::date >= (select period_start from date_range)
    AND c.valid_from::date <= (select period_end from date_range)
    AND c.valid_to::date > current_timestamp
    AND c.basis_of_reimbursement_determination_resp IN ('03', '07', '13')
),
aggregate_fills as (
  select
    case when cr.valid_from::date < (select period_start from date_range) 
         then (select period_start from date_range)
         else cr.valid_from::date end as start_date,
    case when cr.valid_to::date > (select period_end from date_range) 
         then (select period_end from date_range)
         else cr.valid_to::date end as end_date,     
    f.drug_type,
    f.price_basis AS basis_of_reimbursement_source,
    cr.target_rate,
    cr.days_supply_from,
    cr.days_supply_to,
    cr.dfer_target_rate,
    SUM(f.ingredient_cost_paid_resp) AS TotalIngredientCost,
    SUM(f.total_paid_response) AS TotalAdministrationFee,
    SUM(f.dispensing_fee_paid) AS TotalDispensingFee,
    SUM(full_cost) AS "Total Full Cost AWP/MAC/WAC",
    COUNT(*) AS claim_count
from fills f 
left join (
            SELECT * 
            from reporting.chain_rates
            where 
            chain_name = 'cvs'
            and bin_number = '019901'
            and partner = '') cr on
    f.drug_type = cr.drug_type
    and f.days_supply >= cr.days_supply_from
    and f.days_supply <= cr.days_supply_to
    and f.fill_date::date >= cr.valid_from::date AND f.fill_date::date <= cr.valid_to::date
    and f.price_basis = cr.price_basis
group by 1,2,3,4,5,6,7,8
)
SELECT
  to_char((select period_start from date_range), 'MM/DD/YYYY') as period_start,
  to_char((select period_end from date_range) ,'MM/DD/YYYY') AS period_end,
  f.start_date AS start_date,
  f.end_date AS end_date,
  case 
    when f.days_supply_from = 0 and f.days_supply_to = 99999 then 'Any'
    when f.days_supply_to = 99999 then f.days_supply_from || ' +'
  else
    f.days_supply_from || ' - ' || f.days_supply_to
  end AS days_supply,
  f.drug_type AS "Drug Type",
  f.basis_of_reimbursement_source AS basis_of_reimbursement_source,
  isnull(f.claim_count::BIGINT,0)  AS "claim count",
  (isnull(f.TotalIngredientCost, 0) ) / 100::decimal(3,0) AS "Total Ingredient Cost",
  (isnull(f.TotalDispensingFee, 0) ) / 100::decimal(3,0) / "claim count" AS "DFER",
  f.dfer_target_rate,
  "claim count" * ("dfer_target_rate" - "dfer") as "DFER Dollar Variance",
  (-isnull(f.TotalAdministrationFee ,0)) / 100::decimal(3,0) AS "Total Administration Fee",
  ((isnull(f."Total Full Cost AWP/MAC/WAC" ,0) ) / 100::decimal(3, 0))::decimal(10,2)  AS "Total Full Cost",
  ((1 - ("Total Ingredient Cost" / "Total Full Cost")) * 100::decimal(3,0)) AS "Actual Effective Rate", 
  f.target_rate as "Target Effective Rate",
  "Actual Effective Rate" - "Target Effective Rate" as "Effective Rate Variance",
  "Total Full Cost" * ("Actual Effective Rate" - "Target Effective Rate"::decimal(7,5)) / 100::decimal(3, 0) as "IC Dollar Variance",
  isnull("IC Dollar Variance" ,0) + isnull("DFER Dollar Variance" ,0) as "Total Dollar Variance"
FROM
  aggregate_fills f
order by "Drug Type", days_supply
