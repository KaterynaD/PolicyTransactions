{% macro default_dim_deductible() %}

select
'Unknown' deductible_id,
0.0 deductible1,
0.0 deductible2,
{{ loaddate() }}
union all

{% endmacro %}