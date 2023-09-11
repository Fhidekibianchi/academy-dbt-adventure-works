{{
    config(
        severity = 'error'
    )
}}

WITH
    vendas_2011 AS (
        SELECT 
            SUM(valor_total_negociado) AS receita_bruta_vendas
        FROM {{ ref('fct_vendas') }} 
        WHERE DATE(data_pedido) BETWEEN '2011-01-01' AND '2011-12-31'
    )

SELECT receita_bruta_vendas
FROM vendas_2011
WHERE receita_bruta_vendas NOT BETWEEN 12646112.15 AND 12646112.17