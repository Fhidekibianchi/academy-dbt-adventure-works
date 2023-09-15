WITH base_calendario AS (

    SELECT
     CAST(FORMAT_DATE('%F', d) AS DATE) AS id_data
    ,CAST(d AS DATE) AS data
    ,CAST(EXTRACT(YEAR FROM d) AS INT) AS ano
    ,CAST(EXTRACT(WEEK FROM d) AS INT) AS semana_ano
    ,CAST(EXTRACT(DAY FROM d) AS INT) AS dia_ano
    --,EXTRACT(YEAR FROM d) AS fiscal_year
    --,FORMAT_DATE('%Q', d) as fiscal_qtr
    ,CAST(EXTRACT(MONTH FROM d) AS INT) AS mes
    ,CAST(FORMAT_DATE('%B', d) AS STRING) AS nome_mes
    ,CAST(FORMAT_DATE('%w', d) AS INT) AS dia_da_semana
    ,CAST(FORMAT_DATE('%A', d) AS STRING) AS nome_dia_da_semana
    ,CAST((CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN True ELSE False END) AS BOOL) AS dia_util,
FROM (
    SELECT *
    FROM UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2050-01-01', INTERVAL 1 DAY)) AS d )
)
SELECT *
FROM base_calendario


