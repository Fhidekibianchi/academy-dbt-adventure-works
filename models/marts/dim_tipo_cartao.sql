WITH tipo_cartao AS (
    SELECT *
    FROM {{ ref('stg_erp__cartao') }}
)

SELECT *
FROM tipo_cartao