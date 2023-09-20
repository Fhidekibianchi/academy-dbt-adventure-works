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
, motivos_final_num AS (
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
, motivos_final AS (
    SELECT 
        id_pedido
        ,CAST(CASE WHEN preco = 1 THEN 'Preço' ELSE null END AS STRING) AS preco 
        ,CAST(CASE WHEN em_promocao = 1 THEN 'Em_Promoção' ELSE null END AS STRING) AS em_promocao 
        ,CAST(CASE WHEN anuncio_revista = 1 THEN 'Anúncio_Revista' ELSE null END AS STRING) AS anuncio_revista 
        ,CAST(CASE WHEN anuncio_televisao = 1 THEN 'Anúncio_Televisão' ELSE null END AS STRING) AS anuncio_televisao
        ,CAST(CASE WHEN fabricante = 1 THEN 'Fabricante' ELSE null END AS STRING) AS fabricante
        ,CAST(CASE WHEN avaliacoes_clientes = 1 THEN 'Avaliações_de_Clientes' ELSE null END AS STRING) AS avaliacoes_clientes
        ,CAST(CASE WHEN eventos_de_demonstracao = 1 THEN 'Eventos_de_Demoenstração' ELSE null END AS STRING) AS eventos_de_demonstracao
        ,CAST(CASE WHEN patrocinio = 1 THEN 'Patrocínio' ELSE null END AS STRING) AS patrocinio
        ,CAST(CASE WHEN qualidade = 1 THEN 'Qualidade' ELSE null END AS STRING) AS qualidade
        ,CAST(CASE WHEN outros = 1 THEN 'Outros' ELSE null END AS STRING) AS outros
        ,CAST(CASE WHEN tipo_motivo_outros = 1 THEN 'Outros' ELSE null END AS STRING) AS tipo_motivo_outros
        ,CAST(CASE WHEN tipo_motivo_marketing = 1 THEN 'Marketing' ELSE null END AS STRING) AS tipo_motivo_marketing
        ,CAST(CASE WHEN tipo_motivo_promocao = 1 THEN 'Promoção' ELSE null END AS STRING) AS tipo_motivo_promocao
    FROM motivos_final_num
)
, tabela_motivos AS (

    SELECT 
    id_pedido
        ,TRIM(CONCAT(IF(preco IS NOT NULL, preco,''),IF(em_promocao IS NOT NULL,CONCAT('; ',em_promocao) ,''),IF(anuncio_revista IS NOT NULL,CONCAT('; ',anuncio_revista) ,''),IF(anuncio_televisao IS NOT NULL,CONCAT('; ',anuncio_televisao) ,'')
                ,IF(fabricante IS NOT NULL,CONCAT('; ',fabricante) ,''),IF(avaliacoes_clientes IS NOT NULL,CONCAT('; ',avaliacoes_clientes) ,''),IF(eventos_de_demonstracao IS NOT NULL,CONCAT('; ',eventos_de_demonstracao) ,'')
                ,IF(patrocinio IS NOT NULL,CONCAT('; ',patrocinio) ,''),IF(qualidade IS NOT NULL,CONCAT('; ',qualidade) ,''),IF(outros IS NOT NULL,CONCAT('; ',outros) ,''))) AS motivo_compra
        ,TRIM(CONCAT(IF(tipo_motivo_promocao IS NOT NULL, tipo_motivo_promocao,''), IF(tipo_motivo_marketing IS NOT NULL, CONCAT('; ',tipo_motivo_marketing), ''),IF(tipo_motivo_outros IS NOT NULL, CONCAT('; ',tipo_motivo_outros), ''))) AS tipo_motivo_compra
    FROM motivos_final
)
, tabela_motivos_final AS (
    SELECT
        id_pedido
        ,CASE WHEN STARTS_WITH(motivo_compra, ';') = TRUE THEN LTRIM(motivo_compra, ';') ELSE motivo_compra END AS motivo_compra
        ,CASE WHEN STARTS_WITH(tipo_motivo_compra, ';') = TRUE THEN LTRIM(tipo_motivo_compra, ';') ELSE tipo_motivo_compra END AS tipo_motivo_compra
    FROM tabela_motivos
)

SELECT *
FROM tabela_motivos_final