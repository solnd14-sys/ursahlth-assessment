with base_claim_num as (
select
  case 
    when length(min(claim_num) over(partition by member_num, servdt, proc_cd order by trxdt asc)) > 11 
    then left(claim_num, -2) 
    else min(claim_num) over(partition by member_num, servdt, proc_cd order by trxdt asc) 
  end as base_claim_number,
  line_num
from raw_claim_trx_nulls 
)

select
  base_claim_number,
  count(*), -- total rows for sanity check
  count(*) filter( where line_num is null) as null_line_num, --counts records where line number is null
  count(*) filter(where line_num is not null) as non_null_line_num -- counts records where line number is not null
from base_claim_num
group by 1
