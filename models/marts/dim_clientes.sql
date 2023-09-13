WITH 
    clientes AS (
        SELECT *
        FROM {{ ref('stg_erp__clientes') }}
    )

    ,pessoas AS (
        SELECT *
        FROM {{ ref('stg_erp__pessoas') }}
    ) 

    ,lojas AS (
        SELECT *
        FROM {{ ref('stg_erp__lojas') }}
    )

    , pessoa_tratado AS (
        SELECT 
            id_entidade
            ,TRIM(CONCAT(COALESCE(forma_tratamento,' '),' ',primeiro_nome,' ',COALESCE(nome_meio,' '), ' ', sobrenome, ' ', COALESCE(sufixo,' '))) AS nome_cliente
        FROM pessoas
    )

    ,lojas_tratado AS (
        SELECT
             id_entidade
            ,nome_loja AS nome_cliente
        FROM lojas
    )

    ,union_nomes AS (
        SELECT * 
        FROM lojas_tratado
        UNION ALL
        SELECT *
        FROM pessoa_tratado
    )

    , clientes_pessoas_lojas AS (
        SELECT 
            cl.id_pessoa
            ,cl.id_loja
            ,l.nome_loja
        FROM clientes cl
        LEFT JOIN lojas l
            ON cl.id_loja = l.id_entidade
        WHERE id_pessoa IS NOT NULL AND id_loja IS NOT NULL
    )


    , clientes_tratado AS (
        SELECT 
             id_cliente
            ,COALESCE(id_pessoa, id_loja) AS id_entidade
        FROM clientes
    )

    , tabela_clientes AS (

        SELECT 
             c.id_cliente
            ,c.id_entidade
            ,u.nome_cliente
            ,CAST(CASE WHEN pl.nome_loja IS NOT NULL THEN True ELSE False END AS BOOL) AS cliente_possui_loja
            ,pl.nome_loja
        FROM clientes_tratado c
        LEFT JOIN union_nomes u
            ON c.id_entidade = u.id_entidade
        LEFT JOIN clientes_pessoas_lojas pl
            ON c.id_entidade = pl.id_pessoa
    )
    SELECT * FROM tabela_clientes