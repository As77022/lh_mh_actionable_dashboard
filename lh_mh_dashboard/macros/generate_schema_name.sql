{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

       {%- if target.schema != "dev_prod" -%}
            {% if node.fqn[1:-1]|length == 0 %}
                 {{ target.schema }}_{{ default_schema }}
            {% else %}
                {% set prefix = node.fqn[1:-1]|join('_') %}
                 {{ target.schema }}_{{ prefix | trim }}
            {% endif %}

       {% else %} 
            {% if node.fqn[1:-1]|length == 0 %}
                {{ default_schema }}  
            {% else %}
                {% set prefix = node.fqn[1:-1]|join('_') %}
                {{ prefix | trim }}_{{ node.name }}
            {% endif %}
         {% endif %}
    {%- else -%}

       {{ custom_schema_name | trim }}_{{ default_schema }}

    {%- endif -%}

{%- endmacro %}
