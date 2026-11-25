{#-
    Incremental filter with a lookback.
    Reads every row dated within `days` days of the latest date already in the table, not only rows
    after it. The model merges on its unique_key, so re-read rows update in place and do not duplicate.
    Rows that arrive later than `days` days behind the latest date are still missed: run a full refresh.
-#}
{% macro incremental_filter(column_name, days=3) %}
  {% if is_incremental() %}
    WHERE {{ column_name }} > (
      SELECT date_sub(MAX({{ column_name }}), {{ days }}) FROM {{ this }}
    )
  {% endif %}
{% endmacro %}
