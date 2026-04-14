{{
    config({
        "materialized": 'table'
    })
}}

SELECT
    a.date, ROUND(close,4) as close_price,volume, 
    SPLIT(a.ticker, '-')[OFFSET(0)] AS ticker_short,b.sub_asset_class,
    FORMAT_DATE('%A', date) AS day_of_week_name
FROM  {{ source('public', 'asset_prices') }} a
LEFT JOIN  {{ ref('ref_assets')}} b
ON a.ticker = b.ticker
WHERE a.asset_class = 'crypto'

