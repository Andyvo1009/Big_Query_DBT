
{% macro strip_html(column_name) %}
    REGEXP_REPLACE
(
        {{ column_name }},
        r'<[^>]+>',
        ''
    )
{% endmacro %}