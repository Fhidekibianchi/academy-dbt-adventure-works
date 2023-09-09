WITH 
source_pedidos AS (

    SELECT 
         CAST(salesorderid AS INT) AS id_pedido 
        ,CAST(customerid AS INT) AS  id_cliente
        ,CAST(salespersonid AS INT) AS  id_vendedor
        ,CAST(territoryid AS INT) AS  id_territorio 
        ,CAST(billtoaddressid AS INT) AS id_endereco_cobranca
        ,CAST(shiptoaddressid AS INT) AS id_endereco
        ,CAST(shipmethodid AS INT) AS id_metodo_envio
        ,CAST(creditcardid AS INT) AS id_cartao_credito
        ,CAST(currencyrateid AS INT) AS id_cambio
        --,CAST(revisionnumber AS INT) AS 
        ,CAST(orderdate AS TIMESTAMP) AS data_pedido
        ,CAST(duedate AS TIMESTAMP) AS data_faturamento
        ,CAST(shipdate AS TIMESTAMP) AS data_envio
        --,CAST(creditcardapprovalcode AS INT) AS 
        ,CAST(subtotal AS NUMERIC) AS valor_pedido 
        ,CAST(taxamt AS NUMERIC) AS valor_impostos
        ,CAST(freight AS NUMERIC) AS valor_frete
        ,CAST(totaldue AS NUMERIC) AS valor_pedido_total
        ,CAST(status AS INT) AS status
        ,CAST(onlineorderflag AS STRING) AS venda_online
        --,CAST(purchaseordernumber AS STRING) AS  
        ,CAST(accountnumber AS STRING) AS codigo_conta 
        --,CAST(comment AS INT) AS 
        --,CAST(rowguid AS INT) AS 
        --,CAST(modifieddate AS INT) AS 

    FROM  {{ source('erp', 'salesorderheader') }}

)
SELECT *
FROM source_pedidos
