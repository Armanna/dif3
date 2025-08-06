with date_range as (
select 
  dateadd('month',-1,date_trunc('month',current_date))::date AS period_start,
  (DATE_TRUNC('month', current_timestamp) - Interval '1 day')::date AS period_end
  -- '20240902'::date as period_start,
  -- '20240908'::date as period_end
),
fill_processing_sum as (
  select
    date_trunc('month', c.valid_from)::date::text as month,
    count(*) as net_fills,
    sum(cpf.processor_fee) as total_processing_fee
  from reporting.claims c
  join reporting.cardholders pt on c.n_cardholder_id = pt.cardholder_id
  left join reporting.claim_processing_fees cpf on cpf.valid_from = c.valid_from and cpf.rx_id = c.rx_id
  where  fill_status = 'filled'
  and pt.partner = 'GoodRx'
  and pt.created_date <= (select period_end from date_range)
  and cpf.processor_fee != 0
  and c.basis_of_reimbursement_determination_resp != '04' --UNC transactions
  AND c.valid_from::date >= (select period_start from date_range)
  AND c.valid_from::date <= (select period_end from date_range)
  AND c.valid_to::date > (select period_end from date_range)
  group by 1
),
reversal_processing_sum as (
select
  date_trunc('month', c.valid_to)::date::text as month, 
  count(*) as net_reversals,
  --deduct 12 because we're using the fill fee
  --for the reversal we don't get back the original fill fee and we get charged for the reversal
  sum(cpf.processor_fee - 12) as total_processing_fee
  from reporting.claims c
  join reporting.cardholders pt on c.n_cardholder_id = pt.cardholder_id
  left join reporting.claim_processing_fees cpf on cpf.valid_from = c.valid_from and cpf.rx_id = c.rx_id
  where fill_status = 'filled' --need this here as don't want to add in reversed claims
  and pt.partner = 'GoodRx'
  and pt.created_date <= (select period_end from date_range)
  and cpf.processor_fee != 0
  and c.basis_of_reimbursement_determination_resp != '04' --UNC transactions
  and c.valid_from < (select period_start from date_range)
  -- were reversed during this period
  AND c.valid_to::date >= (select period_start from date_range)
  AND c.valid_to::date <= (select  period_end from date_range)
  group by 1
),
fill_processing_total as (
select
  fps.month,
  fps.net_fills - isnull(rps.net_reversals,0) as total_net_fills,
  --This is minus as we're using fills only in this calculation
  fps.total_processing_fee - isnull(rps.total_processing_fee,0) as net_processing_fee,
  (net_processing_fee / total_net_fills)::decimal(10,4) / 100::decimal(3,0) as actual_fee_per_fill,
  actual_fee_per_fill + (0.5::decimal(2,1) * (1.41::decimal(3,2) - actual_fee_per_fill)) as synthetic_fee_per_fill,
  GREATEST(actual_fee_per_fill, synthetic_fee_per_fill) as fee_per_fill
from 
  fill_processing_sum fps 
  left join  reversal_processing_sum rps on fps.month = rps.month
)
--select * from fill_processing_total order by month desc
, 
per_claim_fills as (  
select distinct
  date_trunc('month', c.valid_from)::date as month, 
  c.valid_from,
  c.correlation_id,
  c.product_id,
  c.claim_date_of_service,
  c.npi,
  patient_pay_resp::decimal(10,2) / 100::decimal(3,0) as net_revenue,
  --Need to use when ingredient cost + dispensing fee is 1 cent less than unc because sales tax is not considered in this logic
  (ingredient_cost_paid_resp::decimal(10,2) / 100::decimal(3,0) + (-c.total_paid_response::decimal(18, 4)) / 100::decimal(3,0) + dispensing_fee_paid_resp::decimal(10,2) / 100::decimal(3,0)) as ig,
  drug_cost::decimal(10,2) / 100::decimal(3,0) as net_drug_costs,
  usual_and_customary_charge::decimal(10,2) / 100::decimal(3,0) as unc,
  cpf.processor_fee / 100::decimal(3,0) as processor_fee,
  ps.chain_name,
  --net_revenue minus net drug costs
  total_paid_response::decimal(10,2) / -100::decimal(3,0) as margin,
  mp.ndc11 as feed_ndc,
  ng.npi as feed_npi
  from reporting.claims c
  join reporting.cardholders pt on c.n_cardholder_id = pt.cardholder_id
  left join reporting.claim_processing_fees cpf on cpf.valid_from = c.valid_from and cpf.rx_id = c.rx_id
  left join historical_data.pharmacy_history ps on  ps.id = c.npi AND c.valid_from::timestamp >= ps.valid_from::timestamp AND c.valid_from::timestamp < ps.valid_to::timestamp
  left join historical_data.goodrx_feed_npi_groups_history ng on ng.npi::bigint = c.npi and c.valid_from::timestamp >= ng.valid_from::timestamp AND c.valid_from::timestamp < ng.valid_to::timestamp
  left join historical_data.goodrx_feed_mac_prices_history mp on mp.price_group = ng.price_group and mp.ndc11 = c.product_id and c.valid_from::timestamp >= mp.valid_from::timestamp AND c.valid_from::timestamp < mp.valid_to::timestamp
  where  fill_status = 'filled'
  --and date_trunc('day',"time"::timestamp)::date > '2018-12-18'
  and pt.partner = 'GoodRx'
  and pt.created_date <= (select period_end from date_range)
  and c.basis_of_reimbursement_determination_resp != '04' --UNC transactions
  AND c.valid_from::date >= (select period_start from date_range)
  AND c.valid_from::date <= (select period_end from date_range)
  AND c.valid_to::date > (select period_end from date_range)    
),
aggregate_fills as (  
select
  pc.month, 
  count(*) as fills, 
  sum(net_revenue) as hippo_net_revenue,
  sum(net_drug_costs) as net_drug_costs,
  sum(processor_fee) as processor_fees,
  sum(margin) as margin_a,
  SUM(
    CASE
      -- apply default contract goodrx_margin formula that was active untill September 1st 2024
      WHEN pc.claim_date_of_service::date < '2024-09-01'::date THEN
        CASE 
          WHEN fpt.synthetic_fee_per_fill > fpt.actual_fee_per_fill AND processor_fee != 0 THEN
            CASE 
              WHEN (unc - ig) = 0.01::decimal(3,2) THEN GREATEST(0, ((margin - fee_per_fill) * 0.9::decimal(2,1)))
              WHEN pc.feed_ndc IS NULL THEN GREATEST(0, ((margin - fee_per_fill) * 0.9::decimal(2,1)))
              WHEN pc.feed_npi IS NULL THEN GREATEST(0, ((margin - fee_per_fill) * 0.9::decimal(2,1)))
              WHEN pc.chain_name = 'costco' THEN GREATEST(0, ((margin - fee_per_fill) * 0.9::decimal(2,1)))
            ELSE GREATEST((margin - fee_per_fill) * 0.9::decimal(2,1), 5.70::decimal(3,2))
            END
          ELSE
            CASE 
              WHEN (unc - ig) = 0.01::decimal(3,2) THEN GREATEST(0, ((margin - processor_fee) * 0.9::decimal(2,1)))
              WHEN pc.feed_ndc IS NULL THEN GREATEST(0, ((margin - processor_fee) * 0.9::decimal(2,1)))
              WHEN pc.feed_npi IS NULL THEN GREATEST(0, ((margin - processor_fee) * 0.9::decimal(2,1)))
              WHEN pc.chain_name = 'costco' THEN GREATEST(0, ((margin - processor_fee) * 0.9::decimal(2,1)))
            ELSE GREATEST((margin - processor_fee) * 0.9::decimal(2,1), 5.70::decimal(3,2))
            END
        END
  
      -- apply new formula to calculate goodrx_margin according to the "First Amendment" with GoodRx effective September 1, 2024
      WHEN pc.claim_date_of_service::date >= '2024-09-01'::date THEN
        CASE 
          WHEN chain_name = 'walmart' THEN
            CASE 
              WHEN pc.feed_ndc IS NULL THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              WHEN pc.feed_npi IS NULL THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              ELSE (margin - processor_fee) * 0.97::decimal(3,2)
            END
          ELSE
            CASE 
              WHEN pc.feed_ndc IS NULL THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              WHEN pc.feed_npi IS NULL THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              WHEN (unc - ig) = 0.01::decimal(3,2) THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              ELSE GREATEST((margin - processor_fee) * 0.9::decimal(2,1), 5.70::decimal(3,2))
            END
        END
    END
  ) AS good_margin,
  sum(case when ((unc - ig) = 0.01::decimal(3,2)) then 1 else 0 end) as penny_fills
  from per_claim_fills pc
  left join fill_processing_total fpt on fpt.month = pc.month
  group by 1
),
per_claim_reversals as (  
select distinct
  date_trunc('month', c.valid_from)::date as fill_month, 
  date_trunc('month', c.valid_to)::date as reversal_month, 
  c.valid_from,
  c.product_id,
  c.claim_date_of_service,
  c.npi,
  c.correlation_id,
  patient_pay_resp::decimal(10,2) / 100::decimal(3,0) as net_revenue,
    --Need to use when ingredient cost + dispensing fee is 1 cent less than unc because sales tax is not considered in this logic
  (ingredient_cost_paid_resp::decimal(10,2) / 100::decimal(3,0) + (-c.total_paid_response::decimal(18, 4)) / 100::decimal(3,0) + dispensing_fee_paid_resp::decimal(10,2) / 100::decimal(3,0)) as ig,
  drug_cost::decimal(10,2) / 100::decimal(3,0) as net_drug_costs,
  usual_and_customary_charge::decimal(10,2) / 100::decimal(3,0) as unc,
  cpf.processor_fee / 100::decimal(3,0) as processor_fee,
  ps.chain_name,
  total_paid_response::decimal(10,2) / -100::decimal(3,0)  as margin,
  mp.ndc11 as feed_ndc,
  ng.npi as feed_npi
  from reporting.claims c
  join reporting.cardholders pt on c.n_cardholder_id = pt.cardholder_id
  left join reporting.claim_processing_fees cpf on cpf.valid_from = c.valid_from and cpf.rx_id = c.rx_id
  left join historical_data.pharmacy_history ps on  ps.id = c.npi AND c.valid_from::timestamp >= ps.valid_from::timestamp AND c.valid_from::timestamp < ps.valid_to::timestamp
  left join historical_data.goodrx_feed_npi_groups_history ng on ng.npi::bigint = c.npi and c.valid_from::timestamp >= ng.valid_from::timestamp AND c.valid_from::timestamp < ng.valid_to::timestamp
  left join historical_data.goodrx_feed_mac_prices_history mp on mp.price_group = ng.price_group and mp.ndc11 = c.product_id and c.valid_from::timestamp >= mp.valid_from::timestamp AND c.valid_from::timestamp < mp.valid_to::timestamp
  where fill_status = 'filled' --need this here as don't want to add in reversed claims
  and pt.partner = 'GoodRx'
  and pt.created_date <= (select period_end from date_range)
  and c.basis_of_reimbursement_determination_resp != '04' --UNC transactions
  and c.valid_from < (select period_start from date_range)
  -- were reversed during this period
  AND c.valid_to::date >= (select period_start from date_range)
  AND c.valid_to::date <= (select  period_end from date_range)
),
aggregate_reversals as (  
select
  pc.reversal_month,
  count(*) as reversals, 
  sum(net_revenue) as hippo_net_revenue,
  sum(net_drug_costs) as net_drug_costs,
  sum(processor_fee) as processor_fees,
  sum(margin) as margin_a,
  SUM(
    CASE
      -- apply default contract goodrx_margin formula that was active untill September 1st 2024
      WHEN pc.claim_date_of_service::date < '2024-09-01'::date THEN
        CASE 
          WHEN fpt.synthetic_fee_per_fill > fpt.actual_fee_per_fill AND processor_fee != 0 THEN
            CASE 
              WHEN (unc - ig) = 0.01::decimal(3,2) THEN GREATEST(0, ((margin - fee_per_fill) * 0.9::decimal(2,1)))
              WHEN pc.feed_ndc IS NULL THEN GREATEST(0, ((margin - fee_per_fill) * 0.9::decimal(2,1)))
              WHEN pc.feed_npi IS NULL THEN GREATEST(0, ((margin - fee_per_fill) * 0.9::decimal(2,1)))
              WHEN pc.chain_name = 'costco' THEN GREATEST(0, ((margin - fee_per_fill) * 0.9::decimal(2,1)))
            ELSE GREATEST((margin - fee_per_fill) * 0.9::decimal(2,1), 5.70::decimal(3,2))
            END
          ELSE
            CASE 
              WHEN (unc - ig) = 0.01::decimal(3,2) THEN GREATEST(0, ((margin - processor_fee) * 0.9::decimal(2,1)))
              WHEN pc.feed_ndc IS NULL THEN GREATEST(0, ((margin - processor_fee) * 0.9::decimal(2,1)))
              WHEN pc.feed_npi IS NULL THEN GREATEST(0, ((margin - processor_fee) * 0.9::decimal(2,1)))
              WHEN pc.chain_name = 'costco' THEN GREATEST(0, ((margin - processor_fee) * 0.9::decimal(2,1)))
            ELSE GREATEST((margin - processor_fee) * 0.9::decimal(2,1), 5.70::decimal(3,2))
            END
        END
  
      -- apply new formula to calculate goodrx_margin according to the "First Amendment" with GoodRx effective September 1, 2024
      WHEN pc.claim_date_of_service::date >= '2024-09-01'::date THEN
        CASE 
          WHEN chain_name = 'walmart' THEN
            CASE 
              WHEN pc.feed_ndc IS NULL THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              WHEN pc.feed_npi IS NULL THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              ELSE (margin - processor_fee) * 0.97::decimal(3,2)
            END
          ELSE
            CASE 
              WHEN pc.feed_ndc IS NULL THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              WHEN pc.feed_npi IS NULL THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              WHEN (unc - ig) = 0.01::decimal(3,2) THEN GREATEST(0, (margin - processor_fee) * 0.9::decimal(2,1))
              ELSE GREATEST((margin - processor_fee) * 0.9::decimal(2,1), 5.70::decimal(3,2))
            END
        END
    END
  ) AS good_margin,
   sum(case when ((unc - ig) = 0.01::decimal(3,2)) then 1 else 0 end )as penny_fills
  from per_claim_reversals pc
  left join fill_processing_total fpt on fpt.month = pc.reversal_month
  group by 1
),
final as (
select 
rf.month,
rf.fills,
isnull(rr.reversals,0) as reversals,
rf.penny_fills - isnull(rr.penny_fills,0.0) as penny_fills,
rf.hippo_net_revenue - isnull(rr.hippo_net_revenue,0.0) as net_revenue,
rf.net_drug_costs - isnull(rr.net_drug_costs,0.0) as net_drug_costs,
rf.margin_a - isnull(rr.margin_a,0.0) as net_margin,
rf.good_margin - isnull(rr.good_margin,0.0) as good_margin,
(rf.processor_fees - isnull(rr.processor_fees,0.0)) as processor_costs,
(rf.margin_a - isnull(rr.margin_a,0.0)) 
- (rf.processor_fees - isnull(rr.processor_fees,0.0))
- (rf.good_margin - isnull(rr.good_margin,0.0)) as hippo_margin,
net_margin  / (rf.fills - isnull(rr.reversals,0)) as margin_per_fill,
hippo_margin / (rf.fills - isnull(rr.reversals,0)) as hippo_margin_per_fill
from aggregate_fills rf left join aggregate_reversals rr on rf.month = rr.reversal_month
order by month desc
)
select 
  month,
  fills,
  reversals,
  fills - reversals as "fills less reversals",
  net_margin as "total admin fee",
  round(processor_costs,2) as "processor costs",
  round("total admin fee" - "processor costs",2) as "net admin fee", 
  round(good_margin,2) as "Goodrx Margin Due",  
  penny_fills as "penny fills",
  round(0.9 * "net admin fee",2) as "90% margin",
  margin_per_fill as "per claim margin after penny fills"
from final  
order by month desc
