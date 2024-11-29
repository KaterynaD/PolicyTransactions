{{ config(materialized='incremental',
  unique_key=['deductible_id']) }}



with 

stg_data as (
select distinct
coalesce(stg.deductible1, 0) as deductible1,
coalesce(stg.deductible2, 0) as deductible2
from {{ source('PolicyStats', 'stg_pt') }} stg



where {{ incremental_condition() }}
and  (stg.deductible1<>0 and stg.deductible2<>0) /*default which we can not catch when dim does not exist*/




)



, data as (

{% if  var('load_defaults')   %}

{{ default_dim_deductible() }}

{% endif %}

select
{{ dbt_utils.generate_surrogate_key([
                'deductible1', 'deductible2'
            ])
        }} deductible_id,
deductible1,
deductible2,
{{ loaddate() }}
from stg_data
)
select
data.deductible_id::varchar(50) as deductible_id,
data.deductible1::numeric as deductible1,
data.deductible2::numeric as deductible2,
data.loaddate::timestamp without time zone as loaddate
from data