{{ config(materialized='scd2_plus',
   unique_key='driver_uniqueid',

   check_cols=['gendercd','birthdate','maritalstatuscd','pointschargedterm'],


   updated_at='transactioneffectivedate',
   loaded_at='transactiondate',

   loaddate = var('loaddate'),

   scd_id_col_name = 'driver_id',
   
   scd_valid_from_min_date='1800-01-01',
   scd_valid_to_max_date='3001-12-31'
)
   }}


with data as (
select 
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) driver_uniqueid,
stg.drv_effectivedate transactioneffectivedate,
to_date(cast(stg.transactiondate as varchar), 'YYYYMMDD') as transactiondate,
stg.gendercd as gendercd,
stg.birthdate as birthdate,
stg.maritalstatuscd as maritalstatuscd,
stg.pointschargedterm as pointschargedterm
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}
)
{% if  var('load_defaults')   %}

{{ default_dim_driver() }}

{% endif %}

select
data.driver_uniqueid::varchar(100) as driver_uniqueid,
data.transactioneffectivedate::timestamp without time zone as transactioneffectivedate,
data.transactiondate::date as transactiondate,
data.gendercd::varchar(10) as gendercd,
data.birthdate::date as birthdate,
data.maritalstatuscd::varchar(10) as maritalstatuscd,
data.pointschargedterm::integer as pointschargedterm,
{{ loaddate() }}
from data