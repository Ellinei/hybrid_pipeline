{{ config(materialized='view') }}

with base as (
    select
        symbol,
        timestamp,
        close,
        volume,
        lag(close,   1) over (partition by symbol order by timestamp) as close_prev_1,
        lag(close,  24) over (partition by symbol order by timestamp) as close_prev_24,
        lag(close, 168) over (partition by symbol order by timestamp) as close_prev_168
    from {{ ref('stg_ohlcv') }}
)
select
    symbol,
    timestamp,
    close,
    volume,
    case when close_prev_1   > 0 then ln(close / close_prev_1)   end as log_return_1h,
    case when close_prev_24  > 0 then ln(close / close_prev_24)  end as log_return_24h,
    case when close_prev_168 > 0 then ln(close / close_prev_168) end as log_return_7d,
    case when close_prev_1   > 0 then (close - close_prev_1)   / close_prev_1   * 100 end as pct_change_1h,
    case when close_prev_24  > 0 then (close - close_prev_24)  / close_prev_24  * 100 end as pct_change_24h
from base
