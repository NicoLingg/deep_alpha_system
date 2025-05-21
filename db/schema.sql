-- db/schema.sql
-- Enable TimescaleDB Extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Exchanges Table
CREATE TABLE IF NOT EXISTS exchanges (
    exchange_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    api_url VARCHAR(255), -- Optional: for informational purposes
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Symbols Table
CREATE TABLE IF NOT EXISTS symbols (
    symbol_id SERIAL PRIMARY KEY,
    exchange_id INTEGER NOT NULL REFERENCES exchanges(exchange_id) ON DELETE CASCADE,
    instrument_name VARCHAR(100) NOT NULL, -- Exchange-specific instrument name (e.g., BTCUSDT, BTC-PERP)
    base_asset VARCHAR(30) NOT NULL,    -- Standardized base asset (e.g., BTC)
    quote_asset VARCHAR(30) NOT NULL,   -- Standardized quote asset (e.g., USDT, USD)
    instrument_type VARCHAR(50) NOT NULL, -- Standardized: SPOT, PERP, FUTURE_YYMMDD, OPTION_YYMMDD_STRIKE_TYPE
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (exchange_id, instrument_name) -- Ensures instrument is unique per exchange
);

-- Klines Table (OHLCV)
CREATE TABLE IF NOT EXISTS klines (
    time TIMESTAMPTZ NOT NULL,
    symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id) ON DELETE CASCADE,
    interval VARCHAR(10) NOT NULL,          -- e.g., '1m', '5m', '1h', '1d'
    open_price DECIMAL(30, 15) NOT NULL,    -- Increased precision for some assets
    high_price DECIMAL(30, 15) NOT NULL,
    low_price DECIMAL(30, 15) NOT NULL,
    close_price DECIMAL(30, 15) NOT NULL,
    volume DECIMAL(30, 15) NOT NULL,        -- Base asset volume
    close_time TIMESTAMPTZ NULL,            -- Kline close time from exchange
    quote_asset_volume DECIMAL(30, 15),   -- Quote asset volume
    number_of_trades BIGINT,              -- Changed to BIGINT for larger counts
    taker_buy_base_asset_volume DECIMAL(30, 15),
    taker_buy_quote_asset_volume DECIMAL(30, 15),
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- When this record was ingested/updated
    PRIMARY KEY (time, symbol_id, interval) -- Composite primary key for TimescaleDB hypertable
);

-- Order Book Snapshots Table
DROP TABLE IF EXISTS order_book_snapshots CASCADE; -- Drop if exists for clean recreation during schema apply
CREATE TABLE IF NOT EXISTS order_book_snapshots (
    retrieved_at TIMESTAMPTZ NOT NULL,     -- Timestamp from the exchange, USED FOR HYPERTABLE PARTITIONING
    symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id) ON DELETE CASCADE,
    last_update_id VARCHAR(255) NOT NULL,  -- Exchange's update ID. Kept as VARCHAR for flexibility (some exchanges might not use pure numbers).
    bids JSONB NOT NULL,                   -- Array of [price_str, size_str]
    asks JSONB NOT NULL,                   -- Array of [price_str, size_str]
    ingestion_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- When this record was ingested/recorded locally
);

-- Primary key includes the new partitioning column 'retrieved_at'
ALTER TABLE order_book_snapshots ADD PRIMARY KEY (retrieved_at, symbol_id, last_update_id);


-- Convert to Hypertables
-- Note: Chunk time interval depends on data volume and query patterns. Adjust as needed.
SELECT create_hypertable('klines', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 month');
SELECT create_hypertable('order_book_snapshots', 'retrieved_at', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');


-- Create Indexes
-- Klines
CREATE INDEX IF NOT EXISTS idx_klines_symbol_id_time_interval ON klines (symbol_id, interval, time DESC);
CREATE INDEX IF NOT EXISTS idx_klines_ingested_at ON klines (ingested_at DESC);

-- Order Book Snapshots
CREATE INDEX IF NOT EXISTS idx_order_book_snapshots_symbol_id_retrieved_at ON order_book_snapshots (symbol_id, retrieved_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_book_snapshots_ingestion_time ON order_book_snapshots (ingestion_time DESC);
CREATE INDEX IF NOT EXISTS idx_order_book_snapshots_luid ON order_book_snapshots (last_update_id); -- If querying by LUID

-- For FKs and Joins
CREATE INDEX IF NOT EXISTS idx_symbols_exchange_id ON symbols (exchange_id);
CREATE INDEX IF NOT EXISTS idx_symbols_lookup ON symbols (base_asset, quote_asset, instrument_type);


-- Triggers for updated_at columns
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_exchanges_modtime') THEN
        CREATE TRIGGER update_exchanges_modtime
        BEFORE UPDATE ON exchanges
        FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_symbols_modtime') THEN
        CREATE TRIGGER update_symbols_modtime
        BEFORE UPDATE ON symbols
        FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
    END IF;
END;
$$;

-- ## Continuous Aggregates for Klines (assuming 1m is the base interval ingested) ## --

-- Function to safely create continuous aggregate policies
CREATE OR REPLACE FUNCTION create_cagg_policy_if_not_exists(
    cagg_name TEXT,
    start_offset_interval INTERVAL,
    end_offset_interval INTERVAL,
    schedule_interval_value INTERVAL
)
RETURNS VOID AS $$
DECLARE
    policy_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM timescaledb_information.continuous_aggregate_policies
        WHERE view_name = cagg_name -- In TimescaleDB 2.x, it's materialization_hypertable_name or similar
                                   -- For older versions view_name is correct.
                                   -- Let's assume view_name works or adjust based on TimescaleDB version if error.
                                   -- TimescaleDB 2.x and later: Use timescaledb_information.jobs and check job_type = 'Continuous Aggregate Policy'
    ) INTO policy_exists;

    IF NOT policy_exists THEN
        PERFORM add_continuous_aggregate_policy(
            cagg_name,
            start_offset => start_offset_interval,
            end_offset => end_offset_interval,
            schedule_interval => schedule_interval_value
        );
        RAISE NOTICE 'Created continuous aggregate policy for %', cagg_name;
    ELSE
        RAISE NOTICE 'Continuous aggregate policy for % already exists or check logic failed. Manual check may be needed.', cagg_name;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Template for creating kline CAGGs from 1m base
-- Function to safely create continuous aggregate policies
-- This version simplifies by relying on `if_not_exists` in `add_continuous_aggregate_policy`
CREATE OR REPLACE FUNCTION create_cagg_policy_with_if_not_exists(
    p_cagg_name TEXT,
    p_start_offset_interval INTERVAL,
    p_end_offset_interval INTERVAL,
    p_schedule_interval_value INTERVAL
)
RETURNS VOID AS $$
BEGIN
    -- The if_not_exists parameter handles the check directly in add_continuous_aggregate_policy
    PERFORM add_continuous_aggregate_policy(
        p_cagg_name,
        start_offset => p_start_offset_interval,
        end_offset => p_end_offset_interval,
        schedule_interval => p_schedule_interval_value,
        if_not_exists => TRUE -- Key change here
    );
    RAISE NOTICE 'Ensured continuous aggregate policy for %.', p_cagg_name;

EXCEPTION
    WHEN undefined_function THEN
        RAISE WARNING 'add_continuous_aggregate_policy with if_not_exists might not be supported or an error occurred. Policy for % may not be set optimally. Error: %', p_cagg_name, SQLERRM;
        -- Fallback for very old TimescaleDB versions that might not support if_not_exists
        -- In such a case, the policy might be created multiple times if schema is run repeatedly without DROPPING the CAGG first.
        -- However, since we DROP MATERIALIZED VIEW ... CASCADE, the policy associated with it should also be dropped.
        -- So, a simple add_continuous_aggregate_policy might suffice if the if_not_exists param itself is the issue.
        BEGIN
            PERFORM add_continuous_aggregate_policy(
                p_cagg_name,
                start_offset => p_start_offset_interval,
                end_offset => p_end_offset_interval,
                schedule_interval => p_schedule_interval_value
            );
            RAISE NOTICE 'Fallback: Ensured continuous aggregate policy for % (without if_not_exists).', p_cagg_name;
        EXCEPTION
            WHEN OTHERS THEN
                 RAISE WARNING 'Fallback add_continuous_aggregate_policy also failed for %. Error: %', p_cagg_name, SQLERRM;
        END;
    WHEN OTHERS THEN
        RAISE WARNING 'An unexpected error occurred while setting policy for %. Error: %', p_cagg_name, SQLERRM;
END;
$$ LANGUAGE plpgsql;


-- Template for creating kline CAGGs from 1m base
DO $$
DECLARE
    base_interval TEXT := '1m'; -- Assuming 1m klines are the source
    cagg_definitions CURSOR FOR SELECT view_name, bucket_str, start_off, end_off, schedule_int FROM (VALUES
        ('klines_5min',   '5 minutes',  INTERVAL '3 days', INTERVAL '10 minutes', INTERVAL '5 minutes'),
        ('klines_10min',  '10 minutes', INTERVAL '3 days', INTERVAL '20 minutes', INTERVAL '10 minutes'),
        ('klines_15min',  '15 minutes', INTERVAL '3 days', INTERVAL '30 minutes', INTERVAL '15 minutes'),
        ('klines_30min',  '30 minutes', INTERVAL '3 days', INTERVAL '1 hour',     INTERVAL '30 minutes'),
        ('klines_1hour',  '1 hour',     INTERVAL '7 days', INTERVAL '2 hours',    INTERVAL '1 hour'),
        ('klines_4hour',  '4 hours',    INTERVAL '1 month',INTERVAL '8 hours',    INTERVAL '4 hours'),
        ('klines_6hour',  '6 hours',    INTERVAL '1 month',INTERVAL '12 hours',   INTERVAL '6 hours'),
        ('klines_12hour', '12 hours',   INTERVAL '3 months',INTERVAL '1 day',      INTERVAL '12 hours'),
        ('klines_1day',   '1 day',      INTERVAL '3 months',INTERVAL '2 days',     INTERVAL '1 day')
    ) AS t(view_name, bucket_str, start_off, end_off, schedule_int);
    rec RECORD;
BEGIN
    FOR rec IN cagg_definitions LOOP
        RAISE NOTICE 'Processing CAGG: %', rec.view_name;
        EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS ' || quote_ident(rec.view_name) || ' CASCADE;';
        EXECUTE 'CREATE MATERIALIZED VIEW ' || quote_ident(rec.view_name) ||
        ' WITH (timescaledb.continuous) AS
        SELECT
            time_bucket(''' || rec.bucket_str || ''', time) AS bucket_time,
            symbol_id,
            first(open_price, time) AS open,
            MAX(high_price) AS high,
            MIN(low_price) AS low,
            last(close_price, time) AS close,
            SUM(volume) AS volume,
            last(close_time, time) AS close_time,
            SUM(quote_asset_volume) AS quote_asset_volume,
            SUM(number_of_trades) AS trades,
            SUM(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume,
            SUM(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
        FROM klines
        WHERE interval = ''' || base_interval || '''
        GROUP BY bucket_time, symbol_id
        WITH NO DATA;';

        -- Use the updated function that relies on if_not_exists
        PERFORM create_cagg_policy_with_if_not_exists(
            rec.view_name,
            rec.start_off,
            rec.end_off,
            rec.schedule_int
        );
    END LOOP;
END;
$$;


-- ## REGULAR Materialized Views for Overview Page Performance (Not Continuous Aggregates) ## --

-- Symbol statistics based on 1m klines
DROP MATERIALIZED VIEW IF EXISTS symbol_1m_kline_stats CASCADE;
CREATE MATERIALIZED VIEW symbol_1m_kline_stats AS
SELECT
    s.symbol_id,
    ex.name as exchange_name,
    s.instrument_name,
    s.base_asset,
    s.quote_asset,
    s.instrument_type,
    COUNT(k.time) as kline_1m_count,
    MIN(k.time) as first_1m_kline_time_utc,
    MAX(k.time) as last_1m_kline_time_utc,
    MAX(k.ingested_at) as last_1m_kline_ingested_at_utc
FROM symbols s
JOIN exchanges ex ON s.exchange_id = ex.exchange_id
LEFT JOIN klines k ON s.symbol_id = k.symbol_id AND k.interval = '1m'
GROUP BY s.symbol_id, ex.name, s.instrument_name, s.base_asset, s.quote_asset, s.instrument_type;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS uidx_symbol_1m_kline_stats_symbol_id ON symbol_1m_kline_stats (symbol_id);
CREATE INDEX IF NOT EXISTS idx_symbol_1m_kline_stats_count ON symbol_1m_kline_stats (kline_1m_count DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_symbol_1m_kline_stats_last_time ON symbol_1m_kline_stats (last_1m_kline_time_utc DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_symbol_1m_kline_stats_exchange_quote ON symbol_1m_kline_stats (exchange_name, quote_asset);


-- Daily ingestion stats for 1m klines (based on kline's `ingested_at`)
DROP MATERIALIZED VIEW IF EXISTS daily_1m_klines_ingestion_stats CASCADE;
CREATE MATERIALIZED VIEW daily_1m_klines_ingestion_stats AS
SELECT
    DATE_TRUNC('day', ingested_at AT TIME ZONE 'UTC') AS ingestion_day_utc,
    COUNT(*) as daily_ingested_count
FROM klines
WHERE interval = '1m'
GROUP BY ingestion_day_utc;
CREATE UNIQUE INDEX IF NOT EXISTS uidx_daily_1m_klines_ingestion_stats_day ON daily_1m_klines_ingestion_stats (ingestion_day_utc);


-- Daily ingestion stats for order book snapshots (based on snapshot's `ingestion_time`)
DROP MATERIALIZED VIEW IF EXISTS daily_orderbook_ingestion_stats CASCADE;
CREATE MATERIALIZED VIEW daily_orderbook_ingestion_stats AS
SELECT
    DATE_TRUNC('day', ingestion_time AT TIME ZONE 'UTC') AS ingestion_day_utc, -- Changed from retrieved_at
    COUNT(*) as daily_ingested_count
FROM order_book_snapshots
GROUP BY ingestion_day_utc;
CREATE UNIQUE INDEX IF NOT EXISTS uidx_daily_orderbook_ingestion_stats_day ON daily_orderbook_ingestion_stats (ingestion_day_utc);