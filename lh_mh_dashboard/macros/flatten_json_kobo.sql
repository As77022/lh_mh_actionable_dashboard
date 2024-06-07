{% macro flatten_json_kobo(model_names, json_column) %}
  {% set survey_methods_query %}
    SELECT DISTINCT(jsonb_object_keys({{ json_column }}->'data')) as column_name
    FROM {{ model_names | join(', ') }}
  {% endset %}

  {% set results = run_query(survey_methods_query) %}

  {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
  {% else %}
    {% set results_list = [] %}
  {% endif %}

  SELECT
    _airbyte_ab_id,
    {% for column_name in results_list %}
      {% if column_name not in ['__version__', '_version_'] %}
        {% set alias_base = column_name|replace(' ', '_')|replace('/', '_')|replace('-', '_')|replace('\n', '_')|replace('(', '_')|replace(')', '_')|replace('___', '_')|replace('__', '_') %}
        {% set cleaned_alias = alias_base %}
        {% if column_name|length > 60 %}
          {% set cleaned_alias = alias_base[25:] %}
        {% endif %}
      {% else %}
        {% set cleaned_alias = column_name %}
      {% endif %}
      {{ json_column }}->'data'->>'{{ column_name }}' as "{{ cleaned_alias }}"{% if not loop.last %},{% endif %}
    {% endfor %}
  FROM {{ model_names | join(', ') }}
{% endmacro %}
