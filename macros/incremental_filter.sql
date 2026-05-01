{% macro incremental_filter(column_name, days=3) %}
  {% if is_incremental() %}
    WHERE {{ column_name }} > (select max({{ column_name }}) from {{ this }})
  {% endif %}
{% endmacro %}