{% macro default_dim_limit() %}

select
'Unknown' limit_id,
'~' limit1,
'~' limit2,
{{ loaddate() }}
union all

{% endmacro %}