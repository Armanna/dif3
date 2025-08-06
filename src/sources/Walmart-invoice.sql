-- the monthly invoice has reversals unlike the quarterly. Full thread in slack
-- https://hellohippo.slack.com/files/U7C0CCZBR/F05MV16L08J/re__ext__re__monthly_invoice_and_reversed_claims

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
fills_not_nadac_new AS (
  SELECT 
    (select period_start from date_range) as period_start,
    (select period_end from date_range) as period_end,
    EXTRACT('year' FROM c.valid_from) as year,
    case when c.basis_of_reimbursement_determination_resp = '04' then 'unc'
         when c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response < 0 then 'non_unc_remunerative'
         when c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response >= 0 then 'non_unc_non_remunerative' end as transaction_type,
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
    END AS drug_type,
    'AWP'::text AS basis_of_reimbursement_source,
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.ingredient_cost_paid_resp end) AS "Total Ingredient Cost Paid",
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.total_paid_response end) AS "Total Administration Fee Owed",
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.dispensing_fee_paid_resp end) AS "Total Dispensing Fee Owed",
    COUNT(*) AS claim_count
  FROM reporting.claims c
  LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  -- 12 hours added to claim date of service because default init is at 00:00 AM of the date. While actual prices for that day valid from 10 AM
  LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  --ndc_costs_history data starts on 1 June 2021 so needs backfill for this to revert to prior version
  LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND
    case when c.claim_date_of_service::timestamp < '20210601' then '20210601'::timestamp else c.claim_date_of_service::timestamp end
   + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
  WHERE ph.chain_code = '229'
  AND c.fill_status = 'filled'
  AND c.valid_from::date >= (select period_start from date_range)
  AND c.valid_from::date <= (select period_end from date_range)
  AND c.valid_to::date > (select period_end from date_range)
  AND c.authorization_number NOT ILIKE 'U%'
  AND (
		  	NOT ((ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') OR (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != ''))
		  	OR nch.ndc IS NULL
	)
  GROUP BY 1,2,3,4,5,6
),
reversals_not_nadac_new AS (
  SELECT 
    (select period_start from date_range) as period_start,
    (select period_end from date_range) as period_end,
    EXTRACT('year' FROM c.valid_from) as year,
    case when c.basis_of_reimbursement_determination_resp = '04' then 'unc'
         when c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response < 0 then 'non_unc_remunerative'
         when c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response >= 0 then 'non_unc_non_remunerative' end as transaction_type,
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
    END AS drug_type,
    'AWP'::text AS basis_of_reimbursement_source,
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.ingredient_cost_paid_resp end) AS "Total Ingredient Cost Paid",
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.total_paid_response end) AS "Total Administration Fee Owed",
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.dispensing_fee_paid_resp end) AS "Total Dispensing Fee Owed",
    COUNT(*) AS claim_count
  FROM reporting.claims c
  LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  -- 12 hours added to claim date of service because default init is at 00:00 AM of the date. While actual prices for that day valid from 10 AM
  LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  --ndc_costs_history data starts on 1 June 2021 so needs backfill for this to revert to prior version
  LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND
    case when c.claim_date_of_service::timestamp < '20210601' then '20210601'::timestamp else c.claim_date_of_service::timestamp end
   + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
  WHERE ph.chain_code = '229'
  AND c.fill_status = 'filled'
  AND c.valid_from::date >= '2020-05-01'
  AND c.valid_from < (select period_start from date_range)
  AND c.valid_to::date >= (select period_start from date_range)
  AND c.valid_to::date <= (select  period_end from date_range)
  AND c.authorization_number NOT ILIKE 'U%'
  AND (
		  	NOT ((ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') OR (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != ''))
		  	OR nch.ndc IS NULL
	)
  GROUP BY 1,2,3,4,5,6
),
fills_nadac_new AS (
  SELECT
    (select period_start from date_range) as period_start,
    (select period_end from date_range) as period_end,
    EXTRACT('year' FROM c.valid_from) as year,
    case when c.basis_of_reimbursement_determination_resp = '04' then 'unc'
        when c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response < 0 then 'non_unc_remunerative'
        when c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response >= 0 then 'non_unc_non_remunerative' end as transaction_type,
    CASE 
      WHEN (ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') THEN 'generic'
      WHEN (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != '') THEN 'generic'
      ELSE 'brand'
    END AS drug_type,
    'NADAC'::text AS basis_of_reimbursement_source,
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.ingredient_cost_paid_resp end) AS "Total Ingredient Cost Paid",
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.total_paid_response end) AS "Total Administration Fee Owed",
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.dispensing_fee_paid_resp end) AS "Total Dispensing Fee Owed",
    COUNT(*) AS claim_count
  FROM reporting.claims c
  LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  -- 12 hours added to claim date of service because default init is at 00:00 AM of the date. While actual prices for that day valid from 10 AM
  LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  --ndc_costs_history data starts on 1 June 2021 so needs backfill for this to revert to prior version
  LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND
    case when c.claim_date_of_service::timestamp < '20210601' then '20210601'::timestamp else c.claim_date_of_service::timestamp end
   + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
  WHERE ph.chain_code = '229'
  AND c.fill_status = 'filled'
  AND c.authorization_number NOT ILIKE 'U%'
  AND c.valid_from::date >= (select period_start from date_range)
  AND c.valid_from::date <= (select period_end from date_range)
  AND c.valid_to::date > (select period_end from date_range)
  AND ((ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') OR (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != ''))
  GROUP BY 1,2,3,4,5,6
),
reversals_nadac_new AS (
  SELECT
    (select period_start from date_range) as period_start,
    (select period_end from date_range) as period_end,
    EXTRACT('year' FROM c.valid_from) as year,
    case when c.basis_of_reimbursement_determination_resp = '04' then 'unc'
        when c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response < 0 then 'non_unc_remunerative'
        when c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response >= 0 then 'non_unc_non_remunerative' end as transaction_type,
    CASE 
      WHEN (ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') THEN 'generic'
      WHEN (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != '') THEN 'generic'
      ELSE 'brand'
    END AS drug_type,
    'NADAC'::text AS basis_of_reimbursement_source,
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.ingredient_cost_paid_resp end) AS "Total Ingredient Cost Paid",
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.total_paid_response end) AS "Total Administration Fee Owed",
    SUM(case when c.basis_of_reimbursement_determination_resp = '04' then 0 else c.dispensing_fee_paid_resp end) AS "Total Dispensing Fee Owed",
    COUNT(*) AS claim_count
  FROM reporting.claims c
  LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  -- 12 hours added to claim date of service because default init is at 00:00 AM of the date. While actual prices for that day valid from 10 AM
  LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  --ndc_costs_history data starts on 1 June 2021 so needs backfill for this to revert to prior version
  LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND
    case when c.claim_date_of_service::timestamp < '20210601' then '20210601'::timestamp else c.claim_date_of_service::timestamp end
   + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
  WHERE ph.chain_code = '229'
  AND c.fill_status = 'filled'
  AND c.authorization_number NOT ILIKE 'U%'
  AND c.valid_from::date >= '2020-05-01'
  AND c.valid_from < (select period_start from date_range)
  AND c.valid_to::date >= (select period_start from date_range)
  AND c.valid_to::date <= (select  period_end from date_range)
  AND ((ndcsh.nadac_is_generic = 'True' AND nch.nadac != '') OR (ndcsh.nadac_is_generic = '' AND nch.gpi_nadac != ''))
  GROUP BY 1,2,3,4,5,6
),
ungrouped_f AS (
  SELECT 
    fnt.period_start,
    fnt.period_end,
    fnt.year,
    fnt.transaction_type,
    fnt.drug_type,
    fnt.basis_of_reimbursement_source,
    SUM(fnt."Total Ingredient Cost Paid") AS "Total Ingredient Cost Paid",
    SUM(fnt."Total Administration Fee Owed") AS "Total Administration Fee Owed",
    SUM(fnt."Total Dispensing Fee Owed") AS "Total Dispensing Fee Owed",
    SUM(fnt.claim_count) AS claim_count
  FROM fills_not_nadac_new fnt
  GROUP BY 1,2,3,4,5,6
  UNION
  SELECT 
    fnt.period_start,
    fnt.period_end,
    fnt.year,
    fnt.transaction_type,
    fnt.drug_type,
    fnt.basis_of_reimbursement_source,
    SUM(fnt."Total Ingredient Cost Paid") AS "Total Ingredient Cost Paid",
    SUM(fnt."Total Administration Fee Owed") AS "Total Administration Fee Owed",
    SUM(fnt."Total Dispensing Fee Owed") AS "Total Dispensing Fee Owed",
    SUM(fnt.claim_count) AS claim_count
  FROM fills_nadac_new fnt
  GROUP BY 1,2,3,4,5,6
),
ungrouped_r AS (
  SELECT 
    fnt.period_start,
    fnt.period_end,
    fnt.year,
    fnt.transaction_type,
    fnt.drug_type,
    fnt.basis_of_reimbursement_source,
    SUM(fnt."Total Ingredient Cost Paid") AS "Total Ingredient Cost Paid",
    SUM(fnt."Total Administration Fee Owed") AS "Total Administration Fee Owed",
    SUM(fnt."Total Dispensing Fee Owed") AS "Total Dispensing Fee Owed",
    SUM(fnt.claim_count) AS claim_count
  FROM reversals_not_nadac_new fnt
  GROUP BY 1,2,3,4,5,6
  UNION
  SELECT 
    fnt.period_start,
    fnt.period_end,
    fnt.year,
    fnt.transaction_type,
    fnt.drug_type,
    fnt.basis_of_reimbursement_source,
    SUM(fnt."Total Ingredient Cost Paid") AS "Total Ingredient Cost Paid",
    SUM(fnt."Total Administration Fee Owed") AS "Total Administration Fee Owed",
    SUM(fnt."Total Dispensing Fee Owed") AS "Total Dispensing Fee Owed",
    SUM(fnt.claim_count) AS claim_count
  FROM reversals_nadac_new fnt
  GROUP BY 1,2,3,4,5,6
),
fills AS (
  SELECT 
    fnt.period_start,
    fnt.period_end,
    fnt.year,
    fnt.transaction_type,
    fnt.drug_type,
    fnt.basis_of_reimbursement_source,
    SUM(fnt."Total Ingredient Cost Paid") AS "Total Ingredient Cost Paid",
    SUM(fnt."Total Administration Fee Owed") AS "Total Administration Fee Owed",
    SUM(fnt."Total Dispensing Fee Owed") AS "Total Dispensing Fee Owed",
    SUM(fnt.claim_count) AS claim_count
  FROM ungrouped_f fnt
  GROUP BY 1,2,3,4,5,6
),
reversals AS (
  SELECT 
    fnt.period_start,
    fnt.period_end,
    fnt.year,
    fnt.transaction_type,
    fnt.drug_type,
    fnt.basis_of_reimbursement_source,
    SUM(fnt."Total Ingredient Cost Paid") AS "Total Ingredient Cost Paid",
    SUM(fnt."Total Administration Fee Owed") AS "Total Administration Fee Owed",
    SUM(fnt."Total Dispensing Fee Owed") AS "Total Dispensing Fee Owed",
    SUM(fnt.claim_count) AS claim_count
  FROM ungrouped_r fnt
  GROUP BY 1,2,3,4,5,6
),
total_admin as (
SELECT SUM("Total Administration Fee Owed") AS grand_total
FROM (
    SELECT -"Total Administration Fee Owed"::decimal(12,2) / 100::decimal(3,0) AS "Total Administration Fee Owed" FROM fills
) 
),
total_admin_r as (
SELECT SUM("Total Administration Fee Owed") AS grand_total
FROM (
    SELECT -"Total Administration Fee Owed"::decimal(12,2) / 100::decimal(3,0) AS "Total Administration Fee Owed" FROM reversals
) 
)
SELECT 
  isnull(to_char(f.period_start::date, 'MM/DD/YYYY'),to_char(r.period_start::date, 'MM/DD/YYYY')) as period_start,
  isnull(to_char(f.period_end::date,'MM/DD/YYYY'),to_char(r.period_end::date,'MM/DD/YYYY')) AS period_end,
  isnull(to_char(date_add('day',1, f.period_end::date),'MM/DD/YYYY'),to_char(date_add('day',1, r.period_end::date),'MM/DD/YYYY')) AS invoice_date,
  isnull(to_char(dateadd(day,30, f.period_end::date), 'MM/DD/YYYY'),to_char(dateadd(day,30, r.period_end::date), 'MM/DD/YYYY')) as due_date,
  30 as net,
  31030 + datediff(week,'20210401'::date, current_timestamp::date) as invoice_number, --random number which won't be duplicated  
  f.drug_type AS drug_type, 
  f.basis_of_reimbursement_source AS basis_of_reimbursement_source, 
  to_char(total_admin.grand_total - total_admin_r.grand_total,'9,999,999,999D99') as "Grand Total",
  sum(case when f.transaction_type in ('unc','non_unc_remunerative', 'non_unc_non_remunerative') then f.claim_count::BIGINT else 0 end) 
  - sum(case when r.transaction_type in ('unc','non_unc_remunerative', 'non_unc_non_remunerative') then r.claim_count::BIGINT else 0 end) as "claims count",
  sum(case when f.transaction_type in ('non_unc_remunerative', 'non_unc_non_remunerative') then f.claim_count::BIGINT else 0 end)  
  - sum(case when r.transaction_type in ('non_unc_remunerative', 'non_unc_non_remunerative') then r.claim_count::BIGINT else 0 end)   as "total paid claims",
  sum(case when f.transaction_type in ('non_unc_remunerative') then f.claim_count::BIGINT else 0 end) 
  - sum(case when r.transaction_type in ('non_unc_remunerative') then r.claim_count::BIGINT else 0 end) as "total remunerative paid claims",
  to_char(isnull(sum(f."Total Ingredient Cost Paid"::decimal(12,2) / 100::decimal(3,0)), 0)
  - isnull(sum(r."Total Ingredient Cost Paid"::decimal(12,2) / 100::decimal(3,0)), 0),'999,999,999D99')  AS "Total Ingredient Cost Paid",
  to_char(-isnull( sum(f."Total Administration Fee Owed"::decimal(12,2) / 100::decimal(3,0)) ,0)
  + isnull( sum(r."Total Administration Fee Owed"::decimal(12,2) / 100::decimal(3,0)) ,0),'999,999,999D99')  AS "Total Administration Fee Owed"
  -- to_char(isnull(sum(f."Total Dispensing Fee Owed"::decimal(12,2) / 100::decimal(3,0)), 0),'999,999,999D99')  AS "Total Dispensing Fee Owed"
FROM fills f 
full join reversals r on 
  f.period_start = r.period_start and 
  f.period_end = r.period_end and
  f.year = r.year and 
  f.transaction_type = r.transaction_type and
  f.drug_type = r.drug_type and
  f.basis_of_reimbursement_source = r.basis_of_reimbursement_source
cross join total_admin
cross join total_admin_r
group by 1,2,3,4,5,6,7,8,9
