{{ config(materialized='table') }}

-- Join returns (log/pct changes) with the OHLCV data to access high + low for ATR.
-- We extract only high/low from stg_ohlcv to avoid column-name collisions on
-- close and volume, which both upstream models share.
with returns as (
    select * from {{ ref('int_returns') }}
),
highs_lows as (
    select symbol, timestamp, high, low
    from {{ ref('stg_ohlcv') }}
),
joined as (
    select
        r.symbol,
        r.timestamp,
        r.close,
        r.volume,
        r.log_return_1h,
        r.pct_change_1h,
        r.pct_change_24h,
        h.high,
        h.low
    from returns r
    join highs_lows h using (symbol, timestamp)
),
windowed as (
    select
        symbol,
        timestamp,
        close,
        volume,
        log_return_1h,
        pct_change_1h,
        pct_change_24h,
        -- 14-period high/low for RSI proxy and ATR proxy
        max(close) over w14  as max_close_14,
        min(close) over w14  as min_close_14,
        max(high)  over w14  as high_14,
        min(low)   over w14  as low_14,
        -- Moving averages (SMA used as EMA proxy)
        avg(close) over w12  as ema_12_proxy,
        avg(close) over w26  as ema_26_proxy,
        avg(close) over w20  as sma_20,
        stddev(close) over w20 as stddev_20
    from joined
    window
        w12 as (partition by symbol order by timestamp rows between 11 preceding and current row),
        w14 as (partition by symbol order by timestamp rows between 13 preceding and current row),
        w20 as (partition by symbol order by timestamp rows between 19 preceding and current row),
        w26 as (partition by symbol order by timestamp rows between 25 preceding and current row)
)
select
    symbol,
    timestamp,
    close,
    volume,
    log_return_1h,
    pct_change_1h,
    pct_change_24h,
    -- RSI proxy: position of close within the 14-period high/low range (0–100)
    case
        when (max_close_14 - min_close_14) > 0
        then 100.0 * (close - min_close_14) / (max_close_14 - min_close_14)
        else 50.0
    end as rsi_14_proxy,
    -- MACD line: fast MA minus slow MA
    ema_12_proxy - ema_26_proxy as macd_line,
    -- Bollinger Bands
    sma_20                         as bb_middle,
    sma_20 + (2 * stddev_20)       as bb_upper,
    sma_20 - (2 * stddev_20)       as bb_lower,
    case
        when stddev_20 > 0 then (close - sma_20) / stddev_20
        else 0.0
    end as bb_zscore,
    -- ATR proxy: 14-period high/low range
    high_14 - low_14               as range_14,
    current_timestamp              as computed_at
from windowed
where log_return_1h is not null
