{{ config(materialized='incremental',
  unique_key=['limit_id']) }}



with stg_data as (

select distinct
coalesce(stg.limit1,'~') as limit1,
coalesce(stg.limit2,'~') as limit2
from {{ source('PolicyStats', 'stg_pt') }} stg



where {{ incremental_condition() }}
and  (stg.limit1<>'~' and stg.limit2<>'~') /*default which we can not catch when dim does not exist*/
{% if is_incremental() %}
and  not exists (select 1 from {{ this }} dim where concat(cast(stg.limit1 as varchar), '_', cast(stg.limit2 as varchar))=concat(cast(dim.limit1 as varchar), '_', cast(dim.limit2 as varchar)))
{% endif %}


)
,data as (
{% if  var('load_defaults')   %}

{{ default_dim_limit() }}

{% endif %}
  
select
{{ dbt_utils.generate_surrogate_key([
                'limit1', 'limit2'
            ])
        }}  limit_id,
limit1,
limit2,
{{ loaddate() }}
from stg_data
)
select
data.limit_id::varchar(50) as limit_id,
data.limit1::varchar(100) as limit1,
data.limit2::varchar(100) as limit2,
data.loaddate::timestamp without time zone as loaddate
from data