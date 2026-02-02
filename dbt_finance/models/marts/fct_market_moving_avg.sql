with staging as (
    select * from {{ ref('stg_crypto_prices') }}
),

moving_average as (
    select
        coin_id,
        symbol,
        name,
        price_usd,
        ingested_at,
        
        -- CALCULATION: Average price over the last 7 records (ordered by time)
        -- 'rows between 6 preceding and current row' means: 
        -- "Take this row + the 6 before it = 7 rows total"
        avg(price_usd) over (
            partition by coin_id 
            order by ingested_at 
            rows between 6 preceding and current row
        ) as moving_avg_7d,

        -- Let's also track volatility (standard deviation)
        stddev(price_usd) over (
            partition by coin_id 
            order by ingested_at 
            rows between 6 preceding and current row
        ) as volatility_7d

    from staging
)

select * from moving_average