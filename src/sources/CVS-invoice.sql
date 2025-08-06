-------------------------------------------------------------------------------
-- Author       Svetlin Ralchev
-- Created      2021-04-15
-- Description  It prepares a report that includes the total payment amount, 
--              which shall be summarized by the following and include:
--              - service month
--              - service dates
--              - drug claim type (brand / generic)
--              - total net paid claims
--              - net amount owed
-------------------------------------------------------------------------------
with date_range as (
select 
  dateadd('month',-1,date_trunc('month',current_date))::date AS period_start,
  (DATE_TRUNC('month', current_timestamp) - Interval '1 day')::date AS period_end
),
-- with date_range as (
-- select  
--     '20240401'::date as period_start,
--     '20240415'::date as period_end
-- ),
fills_new AS (
  SELECT
    (select period_start from date_range) as period_start,
    (select period_end from date_range) as period_end,
    EXTRACT('year' FROM c.valid_from) AS year,
    CASE
      WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'unc'
      WHEN c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response < 0 THEN 'non_unc_remunerative'
      WHEN c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response >= 0 THEN 'non_unc_non_remunerative'
    END AS transaction_type,
    CASE
      WHEN c.claim_date_of_service::date < '2024-04-01'::date THEN
        CASE
          WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' THEN 'generic'
          WHEN ndcsh.multi_source_code = 'Y' THEN 'generic'
          ELSE 'brand'
        END
      ELSE
        CASE
          WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' THEN 'generic'
          WHEN ndcsh.multi_source_code = 'Y' THEN 'generic'
          WHEN c.bin_number = '019901' AND ndcsh.name_type_code = 'G' THEN 'generic'
          ELSE 'brand'
        END
    END AS drug_type,
    'AWP' :: text AS basis_of_reimbursement_source,
    SUM(
      CASE
        WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 0
        ELSE c.ingredient_cost_paid_resp
      END
    ) AS "Total Ingredient Cost Paid",
    SUM(
      CASE
        WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 0
        ELSE c.total_paid_response
      END
    ) AS "Total Administration Fee Owed",
    COUNT(*) AS claim_count
  FROM
    reporting.claims c
    LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from :: timestamp >= ph.valid_from :: timestamp AND c.valid_from :: timestamp < ph.valid_to :: timestamp
    LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  WHERE
    ph.chain_code in ('008','039','123','177','207','673')
    and c.bin_number in ('019876','019901','027836')
    AND c.valid_from :: date >= '2021-04-01' --go-live date for CVS contract
    AND c.fill_status = 'filled'
    and c.authorization_number not like 'U%'
    AND c.valid_from::date >= (select period_start from date_range)
    AND c.valid_from::date <= (select period_end from date_range)
    AND c.valid_to::date > (select period_end from date_range)
  GROUP BY 1, 2, 3, 4, 5
  ORDER BY 1 DESC
),
reversals_new AS (
  SELECT 
    (select period_start from date_range) as period_start,
    (select period_end from date_range) as period_end,
    EXTRACT('year' FROM c.valid_to) AS year,
    CASE
      WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'unc'
      WHEN c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response < 0 THEN 'non_unc_remunerative'
      WHEN c.basis_of_reimbursement_determination_resp != '04' AND c.total_paid_response >= 0 THEN 'non_unc_non_remunerative'
    END AS transaction_type,
    CASE
      WHEN c.claim_date_of_service::date < '2024-04-01'::date THEN
        CASE
          WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' THEN 'generic'
          WHEN ndcsh.multi_source_code = 'Y' THEN 'generic'
          ELSE 'brand'
        END
      ELSE
        CASE
          WHEN c.dispense_as_written = 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC' THEN 'generic'
          WHEN ndcsh.multi_source_code = 'Y' THEN 'generic'
          WHEN c.bin_number = '019901' AND ndcsh.name_type_code = 'G' THEN 'generic'
          ELSE 'brand'
        END
    END AS drug_type,
    'AWP' :: text AS basis_of_reimbursement_source,
    SUM(
      CASE
        WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 0
        ELSE c.ingredient_cost_paid_resp
      END
    ) AS "Total Ingredient Cost Paid",
    SUM(
      CASE
        WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 0
        ELSE c.total_paid_response
      END
    ) AS "Total Administration Fee Owed",
    COUNT(*) AS claim_count
  FROM
    reporting.claims c
    LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from :: timestamp >= ph.valid_from :: timestamp AND c.valid_from :: timestamp < ph.valid_to :: timestamp
    LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  WHERE
    ph.chain_code in ('008','039','123','177','207','673')
    and c.bin_number in ('019876','019901','027836')
    AND c.fill_status = 'filled' 
    and c.authorization_number not like 'U%'
    and c.valid_from < (select period_start from date_range)
    -- were reversed during this period
    AND c.valid_to::date >= (select period_start from date_range)
    AND c.valid_to::date <= (select  period_end from date_range)
    AND c.valid_from::date >= '2021-04-01' --go-live date for CVS contract
  GROUP BY 1, 2, 3, 4, 5
  ORDER BY period_start DESC
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
    SUM(fnt.claim_count) AS claim_count
  FROM
    fills_new fnt
  GROUP BY 1, 2, 3, 4, 5,6
),
reversals AS (
  SELECT
    rnt.period_start,
    rnt.period_end,
    rnt.year,
    rnt.transaction_type,
    rnt.drug_type,
    rnt.basis_of_reimbursement_source,
    SUM(rnt."Total Ingredient Cost Paid") AS "Total Ingredient Cost Paid",
    SUM(rnt."Total Administration Fee Owed") AS "Total Administration Fee Owed",
    SUM(rnt.claim_count) AS claim_count
  FROM
    reversals_new rnt
  GROUP BY 1, 2, 3, 4, 5,6
),
total_admin as (
SELECT SUM("Total Administration Fee Owed") AS grand_total
FROM (
    SELECT -"Total Administration Fee Owed"::decimal(12,2) / 100::decimal(3,0) AS "Total Administration Fee Owed" FROM fills
        UNION ALL
    SELECT "Total Administration Fee Owed"::decimal(12,2) / 100::decimal(3,0) AS "Total Administration Fee Owed" FROM reversals
) 
)
SELECT
  to_char(COALESCE(f.period_start, r.period_start)::date, 'MM/DD/YYYY') as period_start,
  to_char(COALESCE(f.period_end, r.period_end)::date,'MM/DD/YYYY') AS period_end,
  to_char(date_add('day',1,COALESCE(f.period_end, r.period_end)::date),'MM/DD/YYYY') AS invoice_date,
  to_char(dateadd(day,30, COALESCE(f.period_end, r.period_end)::date )::date, 'MM/DD/YYYY') as due_date,
  30 as net,
  21030 + datediff(week,'20210401'::date, current_timestamp::date) as invoice_number, --random number which won't be duplicated
  COALESCE(f.drug_type, r.drug_type) AS drug_type,
  COALESCE(f.basis_of_reimbursement_source,r.basis_of_reimbursement_source) AS basis_of_reimbursement_source,
  to_char(total_admin.grand_total,'9,999,999,999D99') as "Grand Total",
  sum(CASE WHEN f.transaction_type IN ('unc','non_unc_remunerative','non_unc_non_remunerative') THEN f.claim_count :: BIGINT ELSE 0 END) 
    - sum(CASE WHEN r.transaction_type IN ('unc','non_unc_remunerative','non_unc_non_remunerative') THEN r.claim_count :: BIGINT ELSE 0 END ) AS "claims count",
  sum(CASE WHEN f.transaction_type IN ('non_unc_remunerative','non_unc_non_remunerative') THEN f.claim_count :: BIGINT ELSE 0 END) 
    - sum(CASE WHEN r.transaction_type IN ('non_unc_remunerative','non_unc_non_remunerative') THEN r.claim_count :: BIGINT ELSE 0 END ) AS "total paid claims",
  sum(CASE WHEN f.transaction_type IN ('non_unc_remunerative') THEN f.claim_count :: BIGINT ELSE 0 END) 
    - sum(CASE WHEN r.transaction_type IN ('non_unc_remunerative') THEN r.claim_count :: BIGINT ELSE 0 END) AS "total remunerative paid claims",
  to_char(isnull(sum(f."Total Ingredient Cost Paid"::decimal(12,2) / 100::decimal(5,2)), 0)
    - isnull(sum(r."Total Ingredient Cost Paid"::decimal(12,2) / 100::decimal(5,2)), 0),'9,999,999,999D99') AS "Total Ingredient Cost Paid",
  to_char(-isnull( sum(f."Total Administration Fee Owed"::decimal(12,2) / 100::decimal(5,2)) ,0)  
    + isnull(sum(r."Total Administration Fee Owed"::decimal(12,2) / 100::decimal(5,2)),0 ), '9,999,999,999D99') AS "Total Administration Fee Owed"
FROM
  fills f
  FULL JOIN reversals r ON f.period_start = r.period_start
  AND f.year = r.year
  AND f.drug_type = r.drug_type
  AND f.transaction_type = r.transaction_type
  AND f.basis_of_reimbursement_source = r.basis_of_reimbursement_source
  cross join total_admin
GROUP BY 1,2,3,4,5,6,7,8,9
