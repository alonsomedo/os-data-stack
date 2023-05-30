
{% macro date_filter_batch(column, date=var('target_date')) %}
    {% if is_incremental() %}
        date({{ column }}) = '{{ date }}'
    {% endif %}
{% endmacro %}