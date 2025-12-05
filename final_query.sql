/*This model addresses the scenario if question . The query is written against the second case (questions 5)
where there are NULL line item values but we have determined that claims either contain all NULL line numbers
on populated line numbers
*/

/* The first two CTEs perform some renaming and standardization to simplify downstream transformations. 
Ideally, in a production environment, these two CTEs would exist as their own views/tables in a staging 
and referenced by downstream models.
*/

with base_claim_num as (
select distinct
  claim_num,
  case
    when length(min(claim_num) over(partition by member_num, servdt, proc_cd order by trxdt asc)) > 11 
    then left(claim_num, -2) 
    else min(claim_num) over(partition by member_num, servdt, proc_cd order by trxdt asc) 
  end as base_claim_number 
from raw_claim_trx_nulls
),

base as (
select 
/* 
What follows are a series of case statement that applies the surrogate key generation logic determined before
based on whether the line number is null or not
*/
    case 
      when line_num is null 
      then md5(concat(base_claim_num.base_claim_number, proc_cd, line_num, trxdt)) 
      else md5(concat(base_claim_num.base_claim_number, proc_cd, trxdt)) 
    end as transaction_id,
    case 
      when line_num is null
      then md5(concat(base_claim_num.base_claim_number, proc_cd)) 
      else md5(concat(base_claim_num.base_claim_number, proc_cd, line_num)) 
    end as claim_service_line_item_id,
    base_claim_num.base_claim_number as claim_header_id,
    member_num as member_number,
    servdt as service_date,
    proc_cd as procedure_code,
    paid_amt as paid_amount,
    raw.claim_num as claim_number,
    line_num as line_number,
    trxdt as transaction_date,
  -- We also apply the case logic here as well to avoid excluding duplicated procedure records from the output
    case 
      when line_num is null 
      then row_number() over( 
        partition by (md5(concat(member_num, servdt, proc_cd, raw.claim_num, trxdt))) 
        order by trxdt) 
      else row_number() over( 
        partition by (md5(concat(member_num, servdt, proc_cd, raw.claim_num, line_num, trxdt))) 
        order by trxdt)
    end as transaction_occurrence_number
from raw_claim_trx_nulls as raw
left join base_claim_num
  on raw.claim_num = base_claim_num.claim_num

),

deduped as (
  select
    transaction_id,
    claim_service_line_item_id,
    claim_header_id,
    member_number,
    service_date,
    procedure_code,
    paid_amount,
    claim_number,
    line_number,
    transaction_date
  from base
  where transaction_occurrence_number = 1
)


select
  claim_header_id,
  claim_service_line_item_id,
  transaction_id,
  member_number,
  service_date,
  procedure_code,
  claim_number,
  paid_amount,
  line_number,
  transaction_date,
  sum(paid_amount) over (partition by claim_service_line_item_id) as service_line_item_paid_amount,
  sum(sum(paid_amount)) over (partition by claim_header_id) as claim_header_paid_amoount
from deduped
group by 
  claim_header_id,
  claim_service_line_item_id,
  transaction_id,
  member_number,
  service_date,
  procedure_code,
  claim_number,
  paid_amount,
  line_number,
  transaction_date
order by claim_header_id, line_number, procedure_code, transaction_date
