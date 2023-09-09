WITH 
source_pedidos_motivos AS  (

    SELECT
         CAST(salesorderid AS INT) AS id_pedido
        ,CAST(salesreasonid AS INT) AS id_motivo_compra
        --,CAST(modifieddate AS INT) AS 
    FROM {{ source('erp', 'salesorderheadersalesreason') }}
)
SELECT * 
FROM source_pedidos_motivos
