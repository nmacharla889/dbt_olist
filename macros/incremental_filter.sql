{% macro incremental_filter(column_name, days=3) %}
  {% if is_incremental() %}
    WHERE {{ column_name }} > (
      SELECT max_val FROM (
        SELECT MAX({{ column_name }}) AS max_val FROM {{ this }}
      )
    )
  {% endif %}
{% endmacro %}