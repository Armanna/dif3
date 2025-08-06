-- We are taking the cost of the balance out of the webmd invoice. 
-- Assuming we make about $6 per fill (might not be that high due to agressive web pricing)
-- first fill we pay them $0 and leave a balance of $4 to be paid off with subsequent fills (we need to reduce invoice by $4.50 as that's what we pay per fill)
-- second fill we pay them $1 and count the balance as paid off (make a dollar each).
-- we want to quote them outstanding balance in the invoice that has to be paide with second fills.

-- when going from 0 fills to 1 fill make first charge
-- when goign from 1 fill to 2 fills make second charge
-- when going from 2 fills to 1 fill refund second charge
-- when going from 1 fill to 0 fills refund first charge 
with webmd_cardh as (
select distinct(bl.cardholder_id) from swamp.balance_hasher_v4 bl left join reporting.cardholders cr on bl.cardholder_id = cr.cardholder_id where cr.partner = 'webmd'
),
claims as (
select 
c.valid_from as event_time,
c.cardholder_id,
1 as fill_count
from reporting.claims c 
join webmd_cardh wb on c.cardholder_id = wb.cardholder_id 
where c.valid_from >= '20231201' --this is when the offer went out
and c.bin_number in ('019876')
and c.fill_status = 'filled'
and c.basis_of_reimbursement_determination_resp != '04'
and c.valid_to > dateadd('month',1,date_trunc('month',c.valid_from))
union all 
select 
c.valid_to as event_time,
c.cardholder_id,
-1 as fill_count
from reporting.claims c 
join webmd_cardh wb on c.cardholder_id = wb.cardholder_id 
where c.valid_from >= '20231201' --this is when the offer went out
and c.bin_number in ('019876')
and c.fill_status = 'filled'
and c.basis_of_reimbursement_determination_resp != '04'
and c.valid_to > dateadd('month',1,date_trunc('month',c.valid_from))  
and c.valid_to < current_timestamp
order by event_time asc 
),
fill_sum_cte as (
select   
event_time,
cardholder_id,
fill_count,
sum(fill_count) over (partition by cardholder_id order by event_time asc
                      rows between unbounded preceding and current row) as fill_sum
from claims 
),
results as (
SELECT
    f.event_time,
    f.cardholder_id,
    f.fill_count,
    f.fill_sum,
    COALESCE(LAG(f.fill_sum) OVER (PARTITION BY f.cardholder_id ORDER BY f.event_time ASC), 0) AS prior_fill_sum,
    case when fill_sum = 1 and prior_fill_sum = 0 then 4.5 
         when fill_sum = 2 and prior_fill_sum = 1 then 3.5 
         when fill_sum = 1 and prior_fill_sum = 2 then -3.5 
         when fill_sum = 0 and prior_fill_sum = 1 then -4.5 
         else 0 end as webmd_deduction,
    case when fill_sum = 1 and prior_fill_sum = 0 then 4
         when fill_sum = 0 and prior_fill_sum = 1 then -4 --go back as if card never used so no balance outstanding
      else 0 end as webmd_remaining_balance
FROM fill_sum_cte f
ORDER BY f.cardholder_id, f.event_time ASC
),
distinct_users as 
(
  SELECT
  date_trunc('month',event_time) as month,
  count(distinct cardholder_id) as balance_users
  from results
  where 
    fill_count = 1
  group by 1
)
select 
  date_trunc('month',event_time)::date::text as month,
  du.balance_users,
  sum(case when fill_count = 1 then 1 else 0 end) as total_fills,
  sum(webmd_deduction) as webmd_deduction,
  sum(webmd_remaining_balance) as webmd_remaining_balance
from results r join distinct_users du on date_trunc('month',r.event_time) = du.month
group by 1,2
order by 1 desc 
