WITH 
produtos AS (
    SELECT *
    FROM {{ ref('stg_erp__produtos') }}
)

, subcategoria_produto AS (
    SELECT *
    FROM {{ ref('stg_erp__subcategorias_de_produtos') }}
)

, categoria_produto AS (
    SELECT *
    FROM {{ ref('stg_erp__categorias_de_produtos') }}
)

,join_produtos AS (
    SELECT
         p.id_produto
        ,p.nome_produto
        ,cp.categoria_produto
        ,sc.subcategoria_produto
        ,p.nivel_estoque_seguranca
        ,p.nivel_para_reabastecer_estoque
        ,p.custo
        ,p.preco_listado
    FROM produtos p
    LEFT JOIN subcategoria_produto sc
        ON p.id_subcategoria_produto = sc.id_subcategoria_produto
    LEFT JOIN categoria_produto cp
        ON sc.id_categoria_produto = cp.id_categoria_produto
)

SELECT *
FROM join_produtos