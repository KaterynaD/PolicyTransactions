{% macro default_dim_driver() %}

select
'Unknown'::varchar(100) driver_uniqueid,
'1900-01-01'::timestamp without time zone transactioneffectivedate,
'1900-01-01'::date transactiondate,
'~'::varchar(10) gendercd,
'1900-01-01'::date birthdate,
'~'::varchar(10) maritalstatuscd,
0::integer pointschargedterm,
{{ loaddate() }}
union all

{% endmacro %}