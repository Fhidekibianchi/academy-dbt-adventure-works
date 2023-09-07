WITH
    erp_cidades AS (
        SELECT *
        FROM {{ ref('stg_erp__cidades') }}
    )

    , erp_estados AS (
        SELECT *
        FROM {{ ref('stg_erp__estados') }}
    )

    , erp_paises AS (
        SELECT * 
        FROM {{ ref('stg_erp__paises') }}
    )

    , join_cidade_estado_pais AS (
        SELECT 
            c.id_endereco
            ,c.endereco
            ,c.cidade
            ,e.codigo_estado
            ,e.estado
            ,e.codigo_pais
            ,p.pais
        FROM erp_cidades c
        LEFT JOIN erp_estados e
            ON c.id_estado = e.id_estado
        LEFT JOIN erp_paises p
            ON e.codigo_pais = p.codigo_pais
    )
SELECT *
FROM join_cidade_estado_pais