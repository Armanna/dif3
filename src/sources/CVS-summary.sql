with date_range as (
select 
  '{period_start}'::date AS period_start,
  '{period_end}'::date AS period_end
   --date_trunc('year', dateadd(year, -1, current_date))::date AS period_start,
   --(DATE_TRUNC('year', current_timestamp) - Interval '1 day')::date AS period_end
),
-- with date_range as (
-- select  
--     '20210301'::date as period_start,
--     '20210531'::date as period_end
-- ),
fills AS (
  SELECT
    c.claim_date_of_service::date as fill_date,
    EXTRACT('year' FROM c.valid_from) AS year,
    CASE
      WHEN enh.reason = 'cvs_exempt_generics' 
        THEN 'exempt generic'
      WHEN ndcsh.multi_source_code = 'Y' and ndcsh.is_otc = 'True' and fh.ndc IS NULL
        THEN 'OTC generic'
      WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' THEN 'generic'
      WHEN ndcsh.multi_source_code = 'Y' and not ndcsh.is_otc = 'True' and fh.ndc IS NULL
        THEN 'generic'
      WHEN ndcsh.multi_source_code = 'Y' and fh.ndc IS NOT NULL
        THEN 'Specialty generic'    
      WHEN ndcsh.multi_source_code != 'Y' and ndcsh.is_otc = 'True' and fh.ndc IS NULL
        THEN 'OTC brand'
      ELSE 'brand'
    END AS drug_type,
    bin_number,
    ingredient_cost_paid_resp,
    total_paid_response,
    days_supply,
    nch.awp::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4) AS AWP
  FROM
    reporting.claims c
    LEFT JOIN reporting.cardholders p on p.cardholder_id = c.n_cardholder_id
    LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from :: timestamp >= ph.valid_from :: timestamp AND c.valid_from :: timestamp < ph.valid_to :: timestamp
    LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.excluded_ndcs_history enh on enh.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= enh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < enh.valid_to::timestamp AND enh.reason = 'cvs_exempt_generics' AND c.bin_number = '019876'
    LEFT JOIN historical_data_pbm_hippo.cvs_tpdt_specialty_list_ndcs fh
        ON c.product_id = fh.ndc AND c.claim_date_of_service::timestamp + interval '12 hours' >= fh.valid_from::timestamp
        AND c.claim_date_of_service::timestamp + interval '12 hours' < fh.valid_to::timestamp
        and c.bin_number = '019901'
  WHERE
    ph.chain_code in ('008','039','123','177','207','673')
    and c.bin_number = '{bin}'
    -- when on main bin need to split webmd from non-webmd. CVS have asked for different bin which will make this less complicated.
    and 1 = case when p.partner = 'webmd'  and '{bin}' = '019876' and '{partner}' = 'webmd' then 1 
                 when p.partner != 'webmd' and '{bin}' = '019876' and '{partner}' = '' then 1
                 -- hard code this row to deal with contract start date
                 when p.partner = 'webmd' and '{bin}' = '019876' and '{partner}' = '' and c.claim_date_of_service::date < '20230121' then 1
                 when '{bin}' != '019876' then 1
                else 0
            end
    AND c.valid_from::date >= '2021-04-01' --go-live date for CVS contract
    AND c.fill_status = 'filled'
    and c.authorization_number not like 'U%%'
    AND c.valid_from::date >= (select period_start from date_range)
    AND c.valid_from::date <= (select period_end from date_range)
    AND c.valid_to::date > current_timestamp
    AND c.basis_of_reimbursement_determination_resp = '03'
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
    'AWP'::text AS basis_of_reimbursement_source,
    cr.target_rate,
    cr.days_supply_from,
    cr.days_supply_to,
    SUM(f.ingredient_cost_paid_resp) AS TotalIngredientCost,
    SUM(f.total_paid_response) AS TotalAdministrationFee,
    SUM(AWP) AS TotalAWP,
    COUNT(*) AS claim_count
from fills f 
left join reporting.chain_rates cr on f.drug_type = cr.drug_type
 and f.days_supply >= cr.days_supply_from
 and f.days_supply <= cr.days_supply_to
 and f.fill_date >= cr.valid_from::date
 and f.fill_date <= cr.valid_to::date
where 
 cr.chain_name = 'cvs'
 and cr.partner = '{partner}'
 and cr.bin_number = f.bin_number
group by 1,2,3,4,5,6,7
),
reversals AS (
select
    c.claim_date_of_service::date as fill_date,
    EXTRACT('year' FROM c.valid_from) AS year,
    CASE
      WHEN ndcsh.multi_source_code = 'Y' and ndcsh.is_otc = 'True' and fh.ndc IS NULL
        THEN 'OTC generic'
      WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' THEN 'generic'
      WHEN ndcsh.multi_source_code = 'Y' and not ndcsh.is_otc = 'True' and fh.ndc IS NULL
        THEN 'generic'
      WHEN ndcsh.multi_source_code = 'Y' and fh.ndc IS NOT NULL
        THEN 'Specialty generic'    
      WHEN ndcsh.multi_source_code != 'Y' and ndcsh.is_otc = 'True' and fh.ndc IS NULL
        THEN 'OTC brand'
      ELSE 'brand'
    END AS drug_type,
    bin_number,
    ingredient_cost_paid_resp,
    total_paid_response,
    days_supply,
    nch.awp::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4) AS AWP
  FROM
    reporting.claims c
    LEFT JOIN reporting.cardholders p on p.cardholder_id = c.n_cardholder_id
    LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from :: timestamp >= ph.valid_from :: timestamp AND c.valid_from :: timestamp < ph.valid_to :: timestamp
    LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
    LEFT JOIN historical_data_bin_019876.excluded_ndcs_history enh on enh.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= enh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < enh.valid_to::timestamp AND enh.reason = 'cvs_exempt_generics' AND c.bin_number = '019876'
    LEFT JOIN historical_data_pbm_hippo.cvs_tpdt_specialty_list_ndcs fh
        ON c.product_id = fh.ndc AND c.claim_date_of_service::timestamp + interval '12 hours' >= fh.valid_from::timestamp
        AND c.claim_date_of_service::timestamp + interval '12 hours' < fh.valid_to::timestamp
        and c.bin_number = '019901'
  WHERE
    ph.chain_code in ('008','039','123','177','207','673')
    and c.bin_number = '{bin}'
    -- when on main bin need to split webmd from non-webmd. CVS have asked for different bin which will make this less complicated.
    and 1 = case when p.partner = 'webmd'  and '{bin}' = '019876' and '{partner}' = 'webmd' then 1 
                 when p.partner != 'webmd' and '{bin}' = '019876' and '{partner}' = '' then 1
                 -- hard code this row to deal with contract start date
                 when p.partner = 'webmd' and '{bin}' = '019876' and '{partner}' = '' and c.claim_date_of_service::date < '20230121' then 1
                 when '{bin}' != '019876' then 1
                else 0
            end
    AND c.fill_status = 'filled' 
    and c.authorization_number not like 'U%%'
    and c.valid_from < (select period_start from date_range)
    -- were reversed during this period
    AND c.valid_to::date >= (select period_start from date_range)
    AND c.valid_to::date <= (select period_end from date_range)
    AND c.valid_from::date >= '2021-04-01' --go-live date for CVS contract
    AND c.basis_of_reimbursement_determination_resp = '03'
),
aggregate_reversals as (
  select
    case when cr.valid_from::date < (select period_start from date_range) 
         then (select period_start from date_range)
         else cr.valid_from::date end as start_date,
    case when cr.valid_to::date > (select period_end from date_range) 
         then (select period_end from date_range)
         else cr.valid_to::date end as end_date,
    r.drug_type,
    'AWP'::text AS basis_of_reimbursement_source,
    cr.target_rate,
    cr.days_supply_from,
    cr.days_supply_to,
    SUM(r.ingredient_cost_paid_resp) AS TotalIngredientCost,
    SUM(r.total_paid_response) AS TotalAdministrationFee,
    SUM(AWP) AS TotalAWP,
    COUNT(*) AS claim_count
from reversals r 
left join reporting.chain_rates cr on r.drug_type = cr.drug_type
 and r.days_supply >= cr.days_supply_from
 and r.days_supply <= cr.days_supply_to
 and r.fill_date >= cr.valid_from::date 
 and r.fill_date <= cr.valid_to::date
where 
 cr.chain_name = 'cvs'
 and cr.partner = '{partner}'
 and cr.bin_number = r.bin_number
group by 1,2,3,4,5,6,7
)
SELECT
  to_char((select period_start from date_range), 'MM/DD/YYYY') as period_start,
  to_char((select period_end from date_range) ,'MM/DD/YYYY') AS period_end,
  COALESCE(f.start_date, r.start_date) AS start_date,
  COALESCE(f.end_date, r.end_date) AS end_date,
  case 
    when COALESCE(f.days_supply_from, r.days_supply_from) = 0 and COALESCE(f.days_supply_to, r.days_supply_to) = 99999 then 'Any'
    when COALESCE(f.days_supply_to, r.days_supply_to) = 99999 then COALESCE(f.days_supply_from, r.days_supply_from) || ' +'
  else
    COALESCE(f.days_supply_from, r.days_supply_from) || ' - ' || COALESCE(f.days_supply_to, r.days_supply_to) 
  end AS days_supply,
  COALESCE(f.drug_type, r.drug_type) AS "Drug Type",
  COALESCE(f.basis_of_reimbursement_source,r.basis_of_reimbursement_source) AS basis_of_reimbursement_source,
  isnull(f.claim_count::BIGINT,0) - isnull(r.claim_count::BIGINT,0) AS "claim count",
  (isnull(f.TotalIngredientCost, 0) - isnull(r.TotalIngredientCost, 0)) / 100::decimal(3,0) AS "Total Ingredient Cost",
  (-isnull(f.TotalAdministrationFee ,0)  + isnull(r.TotalAdministrationFee ,0 )) / 100::decimal(3,0) AS "Total Administration Fee",
  ((isnull(f.TotalAWP ,0)  - isnull(r.TotalAWP,0)) / 100::decimal(3, 0))::decimal(12,2)  AS "Total AWP",
  ((1 - ("Total Ingredient Cost" / "Total AWP")) * 100::decimal(3,0)) AS "Actual Effective Rate", 
  COALESCE(f.target_rate, r.target_rate) as "Target Effective Rate",
  "Actual Effective Rate" - "Target Effective Rate" as "Effective Rate Variance",
  "Total AWP" * ("Actual Effective Rate" - "Target Effective Rate"::decimal(7,5)) / 100::decimal(3, 0) as "Dollar Variance"
FROM
  aggregate_fills f
  FULL OUTER JOIN aggregate_reversals r ON f.start_date = r.start_date
    AND f.drug_type = r.drug_type
    AND f.basis_of_reimbursement_source = r.basis_of_reimbursement_source
    AND f.target_rate = r.target_rate
order by "Drug Type", days_supply
