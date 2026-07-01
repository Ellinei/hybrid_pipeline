{{ config(materialized='view') }}

select
    symbol,
    timestamp::timestamptz          as timestamp,
    open::numeric(20,8)             as open,
    high::numeric(20,8)             as high,
    low::numeric(20,8)              as low,
    close::numeric(20,8)            as close,
    volume::numeric(30,8)           as volume,
    quote_volume::numeric(30,8)     as quote_volume,
    trades,
    taker_buy_vol::numeric(30,8)    as taker_buy_volume
from {{ source('raw', 'ohlcv') }}
where open > 0
  and high > 0
  and low > 0
  and close > 0
  and volume >= 0
  and low <= high
