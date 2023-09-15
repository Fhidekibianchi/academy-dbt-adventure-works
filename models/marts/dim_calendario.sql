WITH calendario AS (
    SELECT *
    FROM {{ ref('stg__calendario') }}
)

SELECT *
FROM calendario