{% macro percent_conversion(value, decimal_places=2) %}
    ROUND({{ value }} * 100, {{ decimal_places }}) || '%' 
{% endmacro %}
