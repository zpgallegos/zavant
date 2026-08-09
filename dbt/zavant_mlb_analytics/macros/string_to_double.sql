{% macro string_to_double_check(value) %}
    case
        when {{ value }} like '%.%' and {{ value }} like '%-%' then null
        else cast({{ value }} as double)
    end
{% endmacro %}
