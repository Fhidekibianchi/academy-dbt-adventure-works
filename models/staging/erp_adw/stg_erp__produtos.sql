WITH 
source_produtos AS (
    SELECT
         CAST(productid AS INT) AS id_produto
        ,CAST(productsubcategoryid AS INT) AS id_subcategoria_produto
        ,CAST(name AS STRING) AS nome_produto
        --,CAST(productnumber AS numero_produto
        --,makeflag 
        --,finishedgoodsflag
        --,color
        ,CAST(safetystocklevel AS INT) AS nivel_estoque_seguranca
        ,CAST(reorderpoint AS INT) AS nivel_para_reabastecer_estoque
        ,CAST(standardcost AS NUMERIC) AS custo
        ,CAST(listprice AS NUMERIC) AS preco_listado
        --,size
        --,sizeunitmeasurecode
        --,weightunitmeasurecode
        --,weight
        --,daystomanufacture
        --,productline
        --,class
        --,style
        --,productmodelid
        --,sellstartdate
        --,sellenddate
        --,discontinueddate
        --,rowguid
        --,modifieddate
    FROM {{ source('erp', 'product') }}

)

SELECT * 
FROM source_produtos
