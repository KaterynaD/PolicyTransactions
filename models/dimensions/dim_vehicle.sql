{{ config(materialized='scd2_plus',
   unique_key='vehicle_uniqueid',

   check_cols=['estimatedannualdistance'],
   punch_thru_cols=['vin','model','modelyr','manufacturer'],

   updated_at='transactioneffectivedate',
   loaded_at='transactiondate',

   loaddate = var('loaddate'),

   scd_id_col_name = 'vehicle_id',
   
   scd_valid_from_min_date='1800-01-01',
   scd_valid_to_max_date='3001-12-31'
)
   }}


with data as (
select 
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) ) vehicle_uniqueid,
stg.veh_effectivedate transactioneffectivedate,
to_date(cast(stg.transactiondate as varchar), 'YYYYMMDD') as transactiondate,
stg.vin as  vin,
stg.model as model,
stg.modelyr as modelyr,
stg.manufacturer as manufacturer,
stg.estimatedannualdistance as estimatedannualdistance
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}
)
{% if  var('load_defaults')   %}

{{ default_dim_vehicle() }}

{% endif %}

select
data.vehicle_uniqueid::varchar(100) as vehicle_uniqueid,
data.transactioneffectivedate::timestamp without time zone as transactioneffectivedate,
data.transactiondate::date as transactiondate,
data.vin::varchar(20) as vin,
data.model::varchar(100) as model,
data.modelyr::varchar(100) as modelyr,
data.manufacturer::varchar(100) as manufacturer,
data.estimatedannualdistance::varchar(100) as estimatedannualdistance,
{{ loaddate() }}
from data