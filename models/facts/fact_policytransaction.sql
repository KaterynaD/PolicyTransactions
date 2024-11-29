{{ config(materialized='incremental',
   unique_key=['policytransaction_id'],
) }}

with data as (
select 
/*-----------------------------------------------------------------------*/
policytransaction_uniqueid as policytransaction_id,
/*-----------------------------------------------------------------------*/        
stg.transactiondate,
stg.accountingdate,
/*-----------------------------------------------------------------------*/
coalesce(dim_transaction.transaction_id,'0') as transaction_id,
stg.transactioneffectivedate,
stg.transactionsequence,
/*-----------------------------------------------------------------------*/
coalesce(dim_policy.policy_id, 'Unknown') as policy_id,
coalesce(dim_vehicle.vehicle_id, 'Unknown') as vehicle_id,
coalesce(dim_driver.driver_id, 'Unknown') as driver_id,
coalesce(dim_coverage.coverage_id, 'Unknown') as coverage_id,
coalesce(dim_limit.limit_id, 'Unknown') as limit_id,
coalesce(dim_deductible.deductible_id, 'Unknown') as deductible_id,
/*-----------------------------------------------------------------------*/
stg.amount
/*-----------------------------------------------------------------------*/
/*=======================================================================*/
from {{ source('PolicyStats', 'stg_pt') }} stg
--
left outer join {{ ref('dim_transaction') }} dim_transaction
on stg.transactioncd=dim_transaction.transactioncd
--
left outer join {{ ref('dim_policy') }} dim_policy
on stg.policy_uniqueid=dim_policy.policy_uniqueid
--
left outer join {{ ref('dim_coverage') }} dim_coverage
on stg.coveragecd=dim_coverage.coveragecd
and stg.subline=dim_coverage.subline
and stg.asl=dim_coverage.asl
--
left outer join {{ ref('dim_limit') }} dim_limit
on stg.limit1=dim_limit.limit1
and stg.limit2=dim_limit.limit2
--
left outer join {{ ref('dim_deductible') }} dim_deductible
on stg.deductible1=dim_deductible.deductible1
and stg.deductible2=dim_deductible.deductible2
--
left outer join {{ ref('dim_driver') }} dim_driver
on concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) = dim_driver.driver_uniqueid
and (stg.drv_effectivedate >= dim_driver.valid_from 
and stg.drv_effectivedate < dim_driver.valid_to)

--
left outer join {{ ref('dim_vehicle') }} dim_vehicle
on concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) ) = dim_vehicle.vehicle_uniqueid
and (stg.veh_effectivedate >= dim_vehicle.valid_from
and stg.veh_effectivedate < dim_vehicle.valid_to)
/*=======================================================================*/


where {{ incremental_condition() }}




)
select
/*-----------------------------------------------------------------------*/
data.policytransaction_id::integer as policytransaction_id,
/*-----------------------------------------------------------------------*/        
data.transactiondate::int as transactiondate,
data.accountingdate::int as accountingdate,
/*-----------------------------------------------------------------------*/
data.transaction_id::varchar(3) as transaction_id,
data.transactioneffectivedate::int as transactioneffectivedate,
data.transactionsequence::int as transactionsequence,
/*-----------------------------------------------------------------------*/
data.policy_id::varchar(50) as policy_id,
data.vehicle_id::varchar(50) as vehicle_id,
data.driver_id::varchar(50) as driver_id,
data.coverage_id::varchar(50) as coverage_id,
data.limit_id::varchar(50) as limit_id,
data.deductible_id::varchar(50) as deductible_id,
/*-----------------------------------------------------------------------*/
data.amount::numeric as amount,
{{ loaddate() }}
from data