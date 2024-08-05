{% macro generate_schema_name_for_env(custom_schema_name=none) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is not none -%}
        {%- if custom_schema_name not in (
            "staging",
            "transform",
        ) and "audit" in target.name -%}
            unaudited
        {%- else -%} {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- else -%} {{ default_schema }}
    {%- endif -%}

{%- endmacro %}

{% macro generate_schema_name(schema_name, node) -%}
    {{ generate_schema_name_for_env(schema_name) }}
{%- endmacro %}
