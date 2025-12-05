/*This model addresses the scenario if question 1. The query is written against the first case
where there aren't NULL line item values
*/

/* The first two CTEs perform some renaming and standardization to simplify downstream transformations. 
Ideally, in a production environment, these two CTEs would exist as their own views/tables in a staging 
and referenced by downstream models.
*/
with base_claim_num as (
select distinct
  claim_num,
  /* Assuming that the "R1" and "A1" behaviors exists consistently throughout this dataset,
  this logic determines a base claim numer (defined: claim number without suffix) which will 
  serve as the claim header ID downstream. It works by determining the first occurence 
  (achieved here by using min aggregation) of a claim number based on transaction date.
  We could assume that claim numbers are unique thorughout the dataset, however, we include 
  member_num, servdt and proc_cd in case this is not true
  */
  case 
    when length(min(claim_num) over(partition by member_num, servdt, proc_cd order by trxdt asc)) > 11  --not a fan of hardcoding this value. Would ideally have a dynamic way to determine the length of the base claim number
    then left(claim_num, -2) -- remove the last two digits if the length is > 11, this is to account for instances where a new procedure is introduced that was not part of the original transaction
    else min(claim_num) over(partition by member_num, servdt, proc_cd order by trxdt asc) -- if length = 11 (i.e the original transaction claim ID, then return that claim ID)
  end as base_claim_number 
from raw_claim_trx_nulls
),

/* This CTE introduces the IDs we will use to aggregate the paid amounts in the final query.
It also renames the fields to be more user friendly. This enables new users of this code to easily
understand the field names and focus on the logic instead of deciphering field names*. It also
clalculated an transaction occurence number that will be used to deduplicate transactions in the final query.
*/

base as(
select 
    md5(concat(member_num, servdt, proc_cd, raw.claim_num, line_num, trxdt)) as transaction_id, -- This generates a a hexadecimal output based off of the hash of concatenated strings. The output can be used as a unique identifier for duplicate records. To identify unique transaction IDs, we need to get to the grain of the line num and transaction date. This is more optimal because these field can be used to perform joins instead of joining to the disparate fields which could slow query performance.
    md5(concat(member_num, servdt, proc_cd, line_num)) as claim_service_line_item_id, -- for line item id, we only need the line item number
    base_claim_num.base_claim_number as claim_header_id, -- we can use the base claim number derived earlier as the unique claim level identifier
    member_num as member_number,
    servdt as service_date,
    proc_cd as procedure_code,
    paid_amt as paid_amount,
    raw.claim_num as claim_number,
    line_num as line_number,
    trxdt as transaction_date,
    row_number() over( partition by (md5(concat(base_claim_num.base_claim_number, proc_cd, raw.claim_num, line_num, trxdt))) order by trxdt) as transaction_occurrence_number -- This calculates a row number for each group of repeated transactions. In the next CTE, we deduplicate these records, by filtering the records to the first occurrence. Need to maintain the raw claim number here to differentiate betwwen reversal and adjustment transactions
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
  where transaction_occurrence_number = 1 -- filters for only the first occurence of a record
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
  sum(paid_amount) over (partition by claim_service_line_item_id) as service_line_item_paid_amount, -- Calculates the "final action" amount for each line item
  sum(sum(paid_amount)) over (partition by claim_header_id) as claim_header_paid_amoount --sums the "final action" amount for each line item to determine the total claim payout
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
order by line_number, transaction_date
