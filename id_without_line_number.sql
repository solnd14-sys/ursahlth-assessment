/*This model addresses the scenario if question . The query is written against the second case (questions 3-4)
where there are NULL line item values
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


base as(
select 
    md5(concat(base_claim_num.base_claim_number, proc_cd, raw.claim_num, trxdt)) as transaction_id, --Assuming that procedure codes are unique to a transaction, we can use the procedure code to generate the transaction id surrogate key. Assuming that procedure code is always populated.
    md5(concat(base_claim_num.base_claim_number, proc_cd)) as claim_service_line_item_id, -- Could be renamed to a procedure id for clarity and consistency
    base_claim_num.base_claim_number as claim_header_id, 
    member_num as member_number,
    servdt as service_date,
    proc_cd as procedure_code,
    paid_amt as paid_amount,
    raw.claim_num as claim_number,
    line_num as line_number,
    trxdt as transaction_date,
    row_number() over( partition by (md5(concat(base_claim_num.base_claim_number, proc_cd, raw.claim_num, trxdt))) order by trxdt) as transaction_occurrence_number --using procedure code here instead of line number doesn't change the occurence logic and an incrementing row number will be generated for each duplicated record along this partition
from raw_claim_trx as raw
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

/* You'll notice in this output that line item number 5 is excluded from the output. This assumed that line number 5 was an error and should not have been included.
A downside to this apprach is that the transactional nature of the source data is erased and there is no record of that transaction erroneous or not.
*/

select
  claim_header_id,
  claim_service_line_item_id,
  transaction_id,
  member_number,
  service_date,
  procedure_code,
  claim_number,
  paid_amount,
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
order by claim_header_id, claim_number, procedure_code, transaction_date
