WITH 

source_detalhes_pedidos  AS (
    SELECT
         CAST(salesorderid AS INT) AS id_pedido
        ,CAST(salesorderdetailid AS INT) AS id_detalhe_pedido
        ,CAST(productid AS INT) AS id_produto
        ,CAST(specialofferid AS INT) AS id_oferta
        --,CAST(carriertrackingnumber AS STRING) AS codigo_acompanhamento_entrega 
        ,CAST(orderqty AS INT) AS quantidade
        ,CAST(unitprice AS NUMERIC) AS preco_unitario
        ,CAST(unitpricediscount AS NUMERIC) AS desconto_percentual_por_unidade
        --,CAST(rowguid AS INT) AS
        --,CAST(modifieddate AS INT) AS
    FROM {{ source('erp', 'salesorderdetail') }}

)
SELECT *
FROM source_detalhes_pedidos
