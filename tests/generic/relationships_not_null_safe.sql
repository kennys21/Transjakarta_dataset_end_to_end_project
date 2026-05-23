{% test relationships_not_null_safe(model, column_name, to, field) %}

SELECT f.{{ column_name }}
FROM {{ model }} f
WHERE f.{{ column_name }} IS NOT NULL
  AND f.{{ column_name }} NOT IN (
      SELECT {{ field }}
      FROM {{ to }}
      WHERE {{ field }} IS NOT NULL
  )

{% endtest %}