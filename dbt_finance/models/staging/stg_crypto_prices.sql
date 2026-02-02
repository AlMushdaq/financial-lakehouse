with source as (
    select * from {{ source('raw_data', 'MARKET_DATA') }}
),

flattened as (
    select
        -- 1. Primary Keys & Timestamp
        record_id,
        ingested_at,
        
        -- 2. Parse JSON Fields (CoinGecko structure)
        json_data:id::varchar as coin_id,
        json_data:symbol::varchar as symbol,
        json_data:name::varchar as name,
        
        -- 3. Numeric Data
        json_data:current_price::float as price_usd,
        json_data:market_cap::float as market_cap,
        json_data:total_volume::float as volume_24h,
        
        -- 4. High/Low & Changes
        json_data:high_24h::float as high_24h,
        json_data:low_24h::float as low_24h,
        json_data:price_change_percentage_24h::float as price_change_pct_24h,
        
        -- 5. Metadata
        json_data:last_updated::timestamp as last_updated_at

    from source
)

select * from flattened