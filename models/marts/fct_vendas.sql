WITH enderecos AS (
    SELECT *
    FROM {{ ref('dim_enderecos') }}
)

, produtos AS (
    SELECT *
    FROM {{ ref('dim_produtos') }}
)

, pedidos AS (
    SELECT *
    FROM {{ ref('int_vendas__pedidos_itens') }}
)

, join_vendas AS (
    SELECT
         pd.pk_vendas
        ,pr.id_produto
        ,ed.id_endereco
        ,pd.id_pedido
        ,pd.id_cliente
        ,pd.id_vendedor
        ,pd.id_territorio
        ,pd.id_metodo_envio
        ,pd.id_cartao_credito
        ,pd.id_cambio
        ,pd.id_detalhe_pedido
        ,pd.id_oferta
        ,pd.quantidade
        ,pd.preco_unitario
        ,pd.desconto_percentual_por_unidade
        ,pd.data_pedido
        ,pd.data_faturamento
        ,pd.data_envio
        ,pd.status
        ,pd.venda_online
        ,pr.nome_produto
        ,pr.categoria_produto
        ,ed.cidade
        ,ed.estado
        ,ed.pais
    FROM pedidos pd
    LEFT JOIN produtos pr
        ON pd.id_produto = pr.id_produto
    LEFT JOIN enderecos ed
        ON pd.id_endereco = ed.id_endereco 
)

    , transformacoes AS (
        SELECT *
        ,(preco_unitario * quantidade) AS valor_total_negociado
        ,(preco_unitario * quantidade) * (1 - desconto_percentual_por_unidade) AS valor_total_negociado_liquido

    )

SELECT *
FROM join_vendas
