{% macro json_extract_if_not_null(json_column, json_path) %}
    CASE 
        WHEN json_exists({{ json_column }}, '{{ json_path }}.@xmlns:xsi') 
        THEN NULL 
        ELSE json_extract_string({{ json_column }}, '{{ json_path }}') 
    END
{% endmacro %}