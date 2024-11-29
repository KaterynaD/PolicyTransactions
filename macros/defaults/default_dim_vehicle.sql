{% macro default_dim_vehicle() %}

select
'Unknown'::varchar(100) vehicle_uniqueid,
'1900-01-01'::timestamp without time zone transactioneffectivedate,
'1900-01-01'::date transactiondate,
'~'::varchar(20) vin,
'~'::varchar(100) model,
'~'::varchar(100) modelyr,
'~'::varchar(100) manufacturer,
'0'::varchar(100) estimatedannualdistance,
{{ loaddate() }}
union all

{% endmacro %}