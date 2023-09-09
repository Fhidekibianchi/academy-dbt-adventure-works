WITH pedidos AS (
    SELECT * 
    FROM {{ ref('stg_erp__pedidos') }}
)

, pedidos_itens AS (
    SELECT * 
    FROM {{ ref('stg_erp__detalhes_pedidos') }}
)

, join_pedidos_itens AS (
    SELECT
         pd.id_pedido 
        ,pd.id_cliente
        ,pd.id_vendedor
        ,pd.id_territorio 
        ,pd.id_endereco
        ,pd.id_metodo_envio
        ,pd.id_cartao_credito
        ,pd.id_cambio
        ,pdi.id_detalhe_pedido
        ,pdi.id_produto
        ,pdi.id_oferta
        ,pdi.quantidade
        ,pdi.preco_unitario
        ,pdi.desconto_percentual_por_unidade
        ,pd.data_pedido
        ,pd.data_faturamento
        ,pd.data_envio
        ,pd.status
        ,pd.venda_online
        --,pd.valor_pedido 
        --,pd.valor_impostos
        --,pd.valor_frete
        --,pd.valor_pedido_total
    FROM pedidos_itens pdi
    LEFT JOIN pedidos pd
        ON pdi.id_pedido = pd.id_pedido
)

, chave_vendas AS (
    SELECT 
        CAST(id_pedido|| '-' || id_produto AS STRING) AS pk_vendas 
        , *
    FROM join_pedidos_itens
)

SELECT *
FROM chave_vendas