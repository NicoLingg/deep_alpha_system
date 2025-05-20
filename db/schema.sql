-- Enable TimescaleDB Extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Exchanges Table
CREATE TABLE IF NOT EXISTS exchanges (
    exchange_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    api_url VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Symbols Table
CREATE TABLE IF NOT EXISTS symbols (
    symbol_id SERIAL PRIMARY KEY,
    exchange_id INTEGER NOT NULL REFERENCES exchanges(exchange_id),
    instrument_name VARCHAR(50) NOT NULL,
    base_asset VARCHAR(20) NOT NULL,
    quote_asset VARCHAR(20) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (exchange_id, instrument_name)
);

-- Klines Table (OHLCV)
CREATE TABLE IF NOT EXISTS klines (
    time TIMESTAMPTZ NOT NULL,
    symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id),
    interval VARCHAR(10) NOT NULL,
    open_price DECIMAL(20, 8) NOT NULL,
    high_price DECIMAL(20, 8) NOT NULL,
    low_price DECIMAL(20, 8) NOT NULL,
    close_price DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 8) NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    quote_asset_volume DECIMAL(20, 8),
    number_of_trades INTEGER,
    taker_buy_base_asset_volume DECIMAL(20, 8),
    taker_buy_quote_asset_volume DECIMAL(20, 8),
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (time, symbol_id, interval)
);

-- Order Book Snapshots Table
CREATE TABLE IF NOT EXISTS order_book_snapshots (
    time TIMESTAMPTZ NOT NULL,
    symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id),
    last_update_id BIGINT NOT NULL,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL,
    retrieved_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (time, symbol_id, last_update_id)
);

-- Convert to Hypertables
SELECT create_hypertable('klines', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 month');
SELECT create_hypertable('order_book_snapshots', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');

-- Create Indexes
CREATE INDEX IF NOT EXISTS idx_klines_symbol_id_time ON klines (symbol_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_order_book_snapshots_symbol_id_time ON order_book_snapshots (symbol_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_klines_interval ON klines (interval);
-- Indexes that will help the REGULAR materialized views for ingestion stats
CREATE INDEX IF NOT EXISTS idx_klines_ingested_at_interval ON klines (ingested_at DESC, interval);
CREATE INDEX IF NOT EXISTS idx_order_book_snapshots_retrieved_at ON order_book_snapshots (retrieved_at DESC);


-- ## Continuous Aggregate ## --
-- Continuous Aggregate for 5-minute klines
DROP MATERIALIZED VIEW IF EXISTS klines_5min CASCADE;
CREATE MATERIALIZED VIEW klines_5min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS bucket_time,
    symbol_id,
    first(open_price, time) AS open,
    MAX(high_price) AS high,
    MIN(low_price) AS low,
    last(close_price, time) AS close,
    SUM(volume) AS volume,
    SUM(number_of_trades) AS trades,
    SUM(quote_asset_volume) AS quote_asset_volume,
    SUM(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume,
    SUM(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
FROM klines
WHERE interval = '1m'
GROUP BY bucket_time, symbol_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('klines_5min',
    start_offset => INTERVAL '3 days', end_offset   => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);

-- Create 10-minute klines view
DROP MATERIALIZED VIEW IF EXISTS klines_10min CASCADE;
CREATE MATERIALIZED VIEW klines_10min
WITH (timescaledb.continuous) AS
SELECT time_bucket('10 minutes', time) AS bucket_time, symbol_id, first(open_price, time) AS open, MAX(high_price) AS high, MIN(low_price) AS low, last(close_price, time) AS close, SUM(volume) AS volume, SUM(number_of_trades) AS trades, SUM(quote_asset_volume) AS quote_asset_volume, SUM(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume, SUM(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
FROM klines WHERE interval = '1m' GROUP BY bucket_time, symbol_id WITH NO DATA;
SELECT add_continuous_aggregate_policy('klines_10min', start_offset => INTERVAL '3 days', end_offset => INTERVAL '10 minutes', schedule_interval => INTERVAL '10 minutes', if_not_exists => TRUE);

-- Create 15-minute klines view
DROP MATERIALIZED VIEW IF EXISTS klines_15min CASCADE;
CREATE MATERIALIZED VIEW klines_15min
WITH (timescaledb.continuous) AS
SELECT time_bucket('15 minutes', time) AS bucket_time, symbol_id, first(open_price, time) AS open, MAX(high_price) AS high, MIN(low_price) AS low, last(close_price, time) AS close, SUM(volume) AS volume, SUM(number_of_trades) AS trades, SUM(quote_asset_volume) AS quote_asset_volume, SUM(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume, SUM(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
FROM klines WHERE interval = '1m' GROUP BY bucket_time, symbol_id WITH NO DATA;
SELECT add_continuous_aggregate_policy('klines_15min', start_offset => INTERVAL '3 days', end_offset => INTERVAL '10 minutes', schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE);

-- Create 30-minute klines view
DROP MATERIALIZED VIEW IF EXISTS klines_30min CASCADE;
CREATE MATERIALIZED VIEW klines_30min
WITH (timescaledb.continuous) AS
SELECT time_bucket('30 minutes', time) AS bucket_time, symbol_id, first(open_price, time) AS open, MAX(high_price) AS high, MIN(low_price) AS low, last(close_price, time) AS close, SUM(volume) AS volume, SUM(number_of_trades) AS trades, SUM(quote_asset_volume) AS quote_asset_volume, SUM(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume, SUM(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
FROM klines WHERE interval = '1m' GROUP BY bucket_time, symbol_id WITH NO DATA;
SELECT add_continuous_aggregate_policy('klines_30min', start_offset => INTERVAL '3 days', end_offset => INTERVAL '10 minutes', schedule_interval => INTERVAL '30 minutes', if_not_exists => TRUE);

-- Continuous Aggregate for 1-hour klines
DROP MATERIALIZED VIEW IF EXISTS klines_1hour CASCADE;
CREATE MATERIALIZED VIEW klines_1hour
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket_time, symbol_id, first(open_price, time) AS open, MAX(high_price) AS high, MIN(low_price) AS low, last(close_price, time) AS close, SUM(volume) AS volume, SUM(number_of_trades) AS trades, SUM(quote_asset_volume) AS quote_asset_volume, SUM(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume, SUM(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
FROM klines WHERE interval = '1m' GROUP BY bucket_time, symbol_id WITH NO DATA;
SELECT add_continuous_aggregate_policy('klines_1hour', start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', if_not_exists => TRUE);

-- Continuous Aggregate for 1-day klines
DROP MATERIALIZED VIEW IF EXISTS klines_1day CASCADE;
CREATE MATERIALIZED VIEW klines_1day
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS bucket_time, symbol_id, first(open_price, time) AS open, MAX(high_price) AS high, MIN(low_price) AS low, last(close_price, time) AS close, SUM(volume) AS volume, SUM(number_of_trades) AS trades, SUM(quote_asset_volume) AS quote_asset_volume, SUM(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume, SUM(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
FROM klines WHERE interval = '1m' GROUP BY bucket_time, symbol_id WITH NO DATA;
SELECT add_continuous_aggregate_policy('klines_1day', start_offset => INTERVAL '3 months', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Create 6-hour klines view
DROP MATERIALIZED VIEW IF EXISTS klines_6hour CASCADE;
CREATE MATERIALIZED VIEW klines_6hour
WITH (timescaledb.continuous) AS
SELECT time_bucket('6 hours', time) AS bucket_time, symbol_id, first(open_price, time) AS open, MAX(high_price) AS high, MIN(low_price) AS low, last(close_price, time) AS close, SUM(volume) AS volume, SUM(number_of_trades) AS trades, SUM(quote_asset_volume) AS quote_asset_volume, SUM(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume, SUM(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
FROM klines WHERE interval = '1m' GROUP BY bucket_time, symbol_id WITH NO DATA;
SELECT add_continuous_aggregate_policy('klines_6hour', start_offset => INTERVAL '3 months', end_offset => INTERVAL '6 hours', schedule_interval => INTERVAL '6 hours', if_not_exists => TRUE);


-- ## Materialized Views for Overview Page Performance ## --

-- Materialized View for Top Symbols by 1m Kline Count (and overall 1m stats)
DROP MATERIALIZED VIEW IF EXISTS symbol_1m_kline_counts CASCADE;
CREATE MATERIALIZED VIEW symbol_1m_kline_counts AS
SELECT
    s.symbol_id,
    s.instrument_name,
    s.base_asset,
    s.quote_asset,
    COUNT(k.time) as kline_1m_count,
    MIN(k.time) as first_1m_kline_time,
    MAX(k.time) as last_1m_kline_time
FROM symbols s
LEFT JOIN klines k ON s.symbol_id = k.symbol_id AND k.interval = '1m'
GROUP BY s.symbol_id, s.instrument_name, s.base_asset, s.quote_asset;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_symbol_1m_kline_counts_symbol_id ON symbol_1m_kline_counts (symbol_id);
CREATE INDEX IF NOT EXISTS idx_symbol_1m_kline_counts_count ON symbol_1m_kline_counts (kline_1m_count DESC NULLS LAST);


-- REGULAR Materialized View for Daily 1m Klines Ingested (based on kline's ingested_at)
DROP MATERIALIZED VIEW IF EXISTS daily_1m_klines_ingested_stats CASCADE;
CREATE MATERIALIZED VIEW daily_1m_klines_ingested_stats AS
SELECT
    DATE_TRUNC('day', ingested_at AT TIME ZONE 'UTC') AS day_bucket,
    COUNT(*) as daily_count
FROM klines
WHERE interval = '1m'
GROUP BY day_bucket;
CREATE INDEX IF NOT EXISTS idx_daily_1m_klines_ingested_stats_day_bucket ON daily_1m_klines_ingested_stats (day_bucket DESC);


-- REGULAR Materialized View for Daily Order Book Snapshots Ingested (based on snapshot's retrieved_at)
DROP MATERIALIZED VIEW IF EXISTS daily_order_book_snapshots_ingested_stats CASCADE;
CREATE MATERIALIZED VIEW daily_order_book_snapshots_ingested_stats AS
SELECT
    DATE_TRUNC('day', retrieved_at AT TIME ZONE 'UTC') AS day_bucket,
    COUNT(*) as daily_count
FROM order_book_snapshots
GROUP BY day_bucket;
CREATE INDEX IF NOT EXISTS idx_daily_order_book_snapshots_ingested_stats_day_bucket ON daily_order_book_snapshots_ingested_stats (day_bucket DESC);