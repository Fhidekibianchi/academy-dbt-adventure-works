WITH pedidos_motivos AS (
    SELECT *
    FROM {{ ref('stg_erp__pedidos_e_motivos') }}
)

, motivos AS (
    SELECT *
    FROM {{ ref('stg_erp__motivos_compra') }}
)

, join_motivos AS (
    SELECT 
        pm.id_pedido
        ,pm.id_motivo_compra
        ,mt.motivo_compra
        ,mt.tipo_motivo_compra
    FROM pedidos_motivos pm
    LEFT JOIN motivos mt
        ON pm.id_motivo_compra = mt.id_motivo_compra
)

, motivos_transpostos AS (
    SELECT 
        id_pedido
        ,CASE WHEN id_motivo_compra = 1 THEN  1
            ELSE 0 END AS price
        ,CASE WHEN id_motivo_compra = 2 THEN  1
            ELSE 0 END AS on_promotion
        ,CASE WHEN id_motivo_compra = 3 THEN  1
            ELSE 0 END AS magazine_advertisement
        ,CASE WHEN id_motivo_compra = 4 THEN  1
            ELSE 0 END AS television_advertisement
        ,CASE WHEN id_motivo_compra = 5 THEN  1
            ELSE 0 END AS manufacturer
        ,CASE WHEN id_motivo_compra = 6 THEN  1
            ELSE 0 END AS review
        ,CASE WHEN id_motivo_compra = 7 THEN  1
            ELSE 0 END AS demo_event
        ,CASE WHEN id_motivo_compra = 8 THEN  1
            ELSE 0 END AS sponsorship
        ,CASE WHEN id_motivo_compra = 9 THEN  1
            ELSE 0 END AS quality
        ,CASE WHEN id_motivo_compra = 10 THEN  1
            ELSE 0 END AS other
        ,CASE WHEN LOWER(tipo_motivo_compra) = "other" THEN  1
            ELSE 0 END AS tipo_motivo_outros
        ,CASE WHEN LOWER(tipo_motivo_compra) = "marketing" THEN  1
            ELSE 0 END AS tipo_motivo_marketing
        ,CASE WHEN LOWER(tipo_motivo_compra) = "promotion" THEN  1
            ELSE 0 END AS tipo_motivo_promocao
    FROM join_motivos    
)
, motivos_final AS (
    SELECT
         DISTINCT(id_pedido) AS id_pedido
        ,SUM(price) AS preco
        ,SUM(on_promotion) AS em_promocao
        ,SUM(magazine_advertisement) AS anuncio_revista
        ,SUM(television_advertisement) AS anuncio_televisao 
        ,SUM(manufacturer) AS fabricante
        ,SUM(review) AS avaliacoes_clientes
        ,SUM(demo_event) AS eventos_de_demonstracao
        ,SUM(sponsorship) AS patrocinio
        ,SUM(quality) AS qualidade
        ,SUM(other) AS outros
        ,CASE WHEN SUM(tipo_motivo_outros) > 0 THEN 1 ELSE 0 END AS tipo_motivo_outros
        ,CASE WHEN SUM(tipo_motivo_marketing) > 0 THEN 1 ELSE 0 END AS tipo_motivo_marketing 
        ,CASE WHEN SUM(tipo_motivo_promocao) > 0 THEN 1 ELSE 0 END AS tipo_motivo_promocao
    FROM motivos_transpostos
    GROUP BY 1
)
SELECT *
FROM motivos_final
