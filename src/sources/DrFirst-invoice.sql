with per_claim_fills as (  
select
  date_trunc('month', c.valid_from)::date as month, 
  claim_date_of_service||c.npi||prescription_reference_number||fill_number as rx_id,
  patient_pay_resp::decimal(10,2) / 100::decimal(3,0) as net_revenue,
  drug_cost::decimal(10,2) / 100::decimal(3,0) as net_drug_costs,
  cpf.processor_fee::decimal(10,2) / 100::decimal(3,0) as processor_fee,
  -total_paid_response::decimal(10,2) / 100::decimal(3,0) as margin
  from reporting.claims c
  join reporting.cardholders pt on c.n_cardholder_id = pt.cardholder_id
  JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  left join reporting.claim_processing_fees cpf on cpf.valid_from = c.valid_from and cpf.rx_id = c.rx_id
  where transaction_response_status in ('P','D') --need this here as don't want to add in reversed claims
  and ph.is_in_network = 'True'
  and pt.partner = '{partner}'
  and c.basis_of_reimbursement_determination_resp != '04' --UNC transactions
    --and were valid through first day of next month
  and dateadd('month',1,date_trunc('month',c.valid_from))  < c.valid_to
),
final_fills as (  
select
  month, 
  count(distinct rx_id) as dr_first_fills, 
  sum(net_revenue) as hippo_net_revenue,
  sum(net_drug_costs) as net_drug_costs,
  sum(margin) as margin,
  sum(processor_fee) as processor_fee,
  sum(margin) - sum(processor_fee) as margin_minus_fees,
  margin_minus_fees * '{percent}'::float as dr_first_pct_margin,
  sum('{minimum}'::float) as dr_first_fill_min_margin
  from per_claim_fills pc
  group by 1
),
results_fills as(
SELECT
 *,
 case when dr_first_pct_margin > dr_first_fill_min_margin then dr_first_pct_margin else dr_first_fill_min_margin end as dr_first_margin,
 case when dr_first_pct_margin > dr_first_fill_min_margin then 'percent' else 'per_fill' end as margin_type
from final_fills
where 
month < date_trunc('month', current_date)::date
order by month desc
),
per_claim_reversals as (  
select
  date_trunc('month', c.valid_from)::date as fill_month, 
  date_trunc('month', c.valid_to)::date as reversal_month, 
  claim_date_of_service||c.npi||prescription_reference_number||fill_number as rx_id,
  patient_pay_resp::decimal(10,2) / 100::decimal(3,0) as net_revenue,
  drug_cost::decimal(10,2) / 100::decimal(3,0) as net_drug_costs,
  cpf.processor_fee::decimal(10,2) / 100::decimal(3,0) as processor_fee,
  -total_paid_response::decimal(10,2) / 100::decimal(3,0) as margin
  from reporting.claims c
  join reporting.cardholders pt on c.n_cardholder_id = pt.cardholder_id
  JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  left join reporting.claim_processing_fees cpf on cpf.valid_from = c.valid_from and cpf.rx_id = c.rx_id
  where fill_status = 'filled' --need this here as don't want to add in reversed claims
  and ph.is_in_network = 'True'
  --and date_trunc('day',"time"::timestamp)::date > '2018-12-18'
  and pt.partner = '{partner}'
  and c.basis_of_reimbursement_determination_resp != '04' --UNC transactions
    --and were valid through first day of next month
  and c.valid_to > dateadd('month',1,date_trunc('month',c.valid_from))  
    --and were reversed
  and c.valid_to < current_timestamp
  ),
final_reversals as (  
select
  fill_month,
  reversal_month,
  count(distinct rx_id) as dr_first_reversals, 
  sum(net_revenue) as hippo_net_revenue,
  sum(net_drug_costs) as net_drug_costs,
  sum(margin) as margin,
  sum(processor_fee) as processor_fee,
  sum(margin) - sum(processor_fee) as margin_minus_fees,
  margin_minus_fees * '{percent}'::float as dr_first_pct_margin,
  sum('{minimum}'::float) as dr_first_fill_min_margin
  from per_claim_reversals pc
  group by 1,2
),
results_reversals as (
SELECT
reversal_month,
sum(fr.dr_first_reversals) as dr_first_reversals,
sum(fr.hippo_net_revenue) as hippo_net_revenue,
sum(fr.net_drug_costs) as net_drug_costs,
sum(fr.margin) as margin,
sum(fr.processor_fee) as processor_fee,
sum(fr.margin_minus_fees) as margin_minus_fees,
sum(case when 
    rf.margin_type = 'percent' then fr.dr_first_pct_margin
else fr.dr_first_fill_min_margin end) as dr_first_reversal_margin
from final_reversals fr join results_fills rf on fr.fill_month = rf.month --use whatever margin we used when the fill happened
where 
reversal_month < date_trunc('month', current_date)::date
group by 1
)
select 
'Q' || to_char(rf.month, 'QYY') as quarter,
rf.month,
rf.dr_first_fills as scripts,
isnull(rr.dr_first_reversals,0) as reversals,
rf.hippo_net_revenue - isnull(rr.hippo_net_revenue,0.0) as net_revenue,
rf.net_drug_costs - isnull(rr.net_drug_costs,0.0) as net_COGS,
rf.processor_fee - isnull(rr.processor_fee,0.0) as transaction_costs,
rf.margin - isnull(rr.margin,0.0) as net_margin,
rf.margin_minus_fees - isnull(rr.margin_minus_fees,0.0) as margin_minus_fees,
dr_first_pct_margin,
dr_first_fill_min_margin,
dr_first_margin as dr_first_fill_margin,
isnull(dr_first_reversal_margin,0.0) as dr_first_reversal_margin,
dr_first_margin - isnull(dr_first_reversal_margin,0.0) as dr_first_margin
from results_fills rf left join results_reversals rr on rf.month = rr.reversal_month
where month > '20230801'
order by month asc
