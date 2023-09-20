WITH tipo_cartao AS (
    SELECT *
    FROM {{ ref('stg_erp__cartao') }}
)
, tipo_nulo AS (

    SELECT 
         'na' AS id_cartao_credito
        ,'Não_utilizou_cartão' AS tipo_cartao
)

, uniao_tabelas AS (

    SELECT *
    FROM tipo_cartao
    UNION ALL
    SELECT *
    FROM tipo_nulo
)

SELECT *
FROM uniao_tabelas
