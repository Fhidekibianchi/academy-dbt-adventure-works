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
        ,pd.quantidade
        ,pd.preco_unitario
        ,pd.desconto_percentual_por_unidade
        --,pd.valor_pedido AS valor_pedido_cheio
        ,pd.valor_impostos AS valor_imposto_pedido_total
        ,pd.valor_frete AS valor_frete_pedido_total
        --,pd.valor_pedido_total
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
        ,valor_frete_pedido_total / COUNT(*) OVER(PARTITION BY id_pedido) AS frete_por_item 
        ,valor_imposto_pedido_total / COUNT(*) OVER(PARTITION BY id_pedido) AS imposto_por_item 
FROM join_vendas
    )

SELECT *
FROM transformacoes
