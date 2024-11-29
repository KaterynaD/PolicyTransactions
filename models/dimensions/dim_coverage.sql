{{ config(materialized='incremental',
  unique_key=['coverage_id'])}}

with data as (
{% if  var('load_defaults')   %}

{{ default_dim_coverage() }}

{% endif %}

select distinct
{{ dbt_utils.generate_surrogate_key([
                'coveragecd', 
                'subline',
                'asl'
            ])
        }} coverage_id,
coalesce(stg.coveragecd,'~') as coveragecd,
coalesce(stg.subline,'~') as subline,
coalesce(stg.asl,'~') as asl,
{{ loaddate() }}
from {{ source('PolicyStats', 'stg_pt') }} stg



where {{ incremental_condition() }}


)
select
data.coverage_id::varchar(50) as coverage_id,
data.coveragecd::varchar(50) as coveragecd,
data.subline::varchar(20) as subline,
data.asl::varchar(20) as asl,
data.loaddate::timestamp without time zone as loaddate
from data