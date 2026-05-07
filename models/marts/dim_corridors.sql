WITH deduplicated_corridors AS (
    SELECT
        corridor_id,
        MAX(corridor_name) AS corridor_name
    FROM {{ ref('stg_transjakarta') }}
    WHERE corridor_id IS NOT NULL
    GROUP BY corridor_id
)

SELECT * FROM deduplicated_corridors