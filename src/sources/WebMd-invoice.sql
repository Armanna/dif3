--1) $4.50 for the first 15,000 Compensable Claims in a given month; 2) 50% of the gross profit received by Hippo from the pharmacy for
--Compensable Claims above 15,000 in a given monthor or $3.00, whichever is higher 
-- for partial paid claims we'll give them 50% of the profit regardless of claim volume. Let's not count the penny fills towards the 15K
with date_range as (
  select
  dateadd('month',-1,date_trunc('month',current_date))::date AS period_start,
  (DATE_TRUNC('month', current_timestamp) - Interval '1 day')::date AS period_end
),
fills_raw as (
SELECT 
  (-c.total_paid_response::decimal(10,2) - cpf.processor_fee::decimal(10,2)) /100::decimal(3,0) as gross_profit,
    --Need to use when ingredient cost + dispensing fee is 1 cent less than unc because sales tax is not considered in this logic
  (ingredient_cost_paid_resp::decimal(10,2) / 100::decimal(3,0) + (-c.total_paid_response::decimal(18, 4)) / 100::decimal(3,0) + dispensing_fee_paid_resp::decimal(10,2) / 100::decimal(3,0)) as ig,
  usual_and_customary_charge::decimal(10,2) / 100::decimal(3,0) as unc,
  --don't take penny fills into account with fill count
  SUM(CASE WHEN (unc - ig) = 0.01::decimal(3,2) THEN 0 ELSE 1 END) 
    OVER (ORDER BY c.valid_from ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS row_num
  FROM
    reporting.claims c
    join reporting.cardholders p on c.cardholder_id = p.cardholder_id
    left join reporting.claim_processing_fees cpf on cpf.valid_from = c.valid_from and cpf.rx_id = c.rx_id
  WHERE
    c.bin_number in ('019876')    
    AND c.fill_status = 'filled'
    AND p.partner = 'webmd'
    AND c.valid_from::date >= (select period_start from date_range)
    AND c.valid_from::date <= (select period_end from date_range)
    AND c.valid_to::date > (select period_end from date_range)
    AND c.basis_of_reimbursement_determination_resp != '04'
),
fills AS (
  SELECT
    (select period_start from date_range) as month,
    SUM(gross_profit) as gross_fill_profit,
    SUM(CASE WHEN row_num > 15000 and (unc - ig) != 0.01::decimal(3,2) THEN gross_profit ELSE 0 END) AS gross_profit_from_15000_onwards,
    SUM(case when (unc - ig) = 0.01::decimal(3,2) THEN gross_profit else 0 end) AS gross_penny_fill_profit,
    SUM(case when (unc - ig) = 0.01::decimal(3,2) THEN 1 else 0 end) AS penny_fills,
    COUNT(*) AS fills
  FROM
    fills_raw c
  GROUP BY 1
),
reversals_raw AS (
  SELECT 
    (c.total_paid_response::decimal(10,2) - cpf.processor_fee::decimal(10,2))/100::decimal(3,0) as gross_profit,
      --Need to use when ingredient cost + dispensing fee is 1 cent less than unc because sales tax is not considered in this logic
    (ingredient_cost_paid_resp::decimal(10,2) / 100::decimal(3,0) + (-c.total_paid_response::decimal(18, 4)) / 100::decimal(3,0) + dispensing_fee_paid_resp::decimal(10,2) / 100::decimal(3,0)) as ig,
    usual_and_customary_charge::decimal(10,2) / 100::decimal(3,0) as unc,
    --don't take penny fills into account with reversal count
    SUM(CASE WHEN (unc - ig) = 0.01::decimal(3,2) THEN 0 ELSE 1 END) 
      OVER (ORDER BY c.valid_from ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS row_num    
  FROM
    reporting.claims c
    join reporting.cardholders p on c.cardholder_id = p.cardholder_id
    left join reporting.claim_processing_fees cpf on cpf.valid_from = c.valid_from and cpf.rx_id = c.rx_id
  WHERE
    c.bin_number in ('019876')    
    AND c.fill_status = 'filled'
    AND p.partner = 'webmd'
    and c.valid_from < (select period_start from date_range)
    -- were reversed during this period
    AND c.valid_to::date >= (select period_start from date_range)
    AND c.valid_to::date <= (select  period_end from date_range)
    AND c.basis_of_reimbursement_determination_resp != '04'
),
reversals AS (
  SELECT 
    (select period_start from date_range) as month,
    SUM(gross_profit) AS gross_reversal_profit,
    SUM(CASE WHEN row_num > 15000 THEN gross_profit ELSE 0 END) AS gross_profit_from_15000_onwards,
    SUM(case when (unc - ig) = 0.01::decimal(3,2) THEN gross_profit else 0 end) AS gross_penny_reversal_profit,
    SUM(case when (unc - ig) = 0.01::decimal(3,2) THEN 1 else 0 end) AS penny_reversals,
    COUNT(*) AS reversals
  FROM
    reversals_raw c
  GROUP BY 1
)
SELECT
  f.month::text,
  f.fills,
  isnull(r.reversals,0) as reversals,
  f.fills - isnull(r.reversals,0) as net_fills,
  f.gross_fill_profit,
  isnull(r.gross_reversal_profit,0.0) as gross_reversal_profit,
  f.gross_fill_profit - isnull(r.gross_reversal_profit,0.0) as gross_profit,
  f.penny_fills,
  isnull(r.penny_reversals,0) as penny_reversals,
  f.penny_fills - isnull(r.penny_reversals,0) as net_penny_fills,
  f.gross_penny_fill_profit,
  isnull(r.gross_penny_reversal_profit) as gross_penny_reversal_profit,
  f.gross_penny_fill_profit - isnull(r.gross_penny_reversal_profit) as net_penny_fill_profit,
  (f.gross_profit_from_15000_onwards - isnull(r.gross_profit_from_15000_onwards,0.0)) * 0.5::decimal(2,1) as fifty_pct_of_profit_over_15000,
  case when (net_fills - net_penny_fills) <= 15000 then 4.5::decimal(2,1) * (net_fills - net_penny_fills) 
        else 67500 + net_penny_fill_profit * 0.5::decimal(2,1) +
        GREATEST((f.gross_profit_from_15000_onwards - isnull(r.gross_profit_from_15000_onwards,0.0)) * 0.5::decimal(2,1), 
                              ((net_fills - net_penny_fills) - 15000) * 3.0::decimal(2,1)) end as webmd_margin
FROM
  fills f
  FULL JOIN reversals r ON f.month = r.month