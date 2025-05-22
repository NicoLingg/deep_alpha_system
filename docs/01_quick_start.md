# Deep Alpha System: Quick Start Tutorial

This tutorial will guide you through the initial setup of the Deep Alpha System, including:
1.  Setting up the configuration.
2.  Initializing the database.
3.  Populating the database with specific symbol definitions (e.g., BTC-USDT, ETC-USDT).
4.  Fetching historical 1-minute kline (candlestick) data for these symbols.
5.  Refreshing data aggregates.

We will be using `make` commands defined in the `Makefile` for these operations.

## Prerequisites

Before you begin, ensure you have the following installed:
*   **Git:** For cloning the repository.
*   **Docker & Docker Compose:** For running the TimescaleDB database.
*   **Python 3:** (Preferably 3.8+ with `pip` and `venv`).
*   **Make:** (Usually pre-installed on Linux/macOS. Windows users might need to install it, e.g., via Chocolatey or WSL).
*   The project repository cloned to your local machine.

## Step 1: Configuration

The system relies on a configuration file, typically `config/config.ini`, for various settings including database credentials, API keys (if needed by specific exchange integrations), and default parameters.

1.  **Copy the example configuration:**
   
```bash
cp config/config.ini.example config/config.ini
```

2.  **Review and Edit `config/config.ini`:**
Open `config/config.ini` in a text editor. Pay attention to:
    *   **`[database]` section:**
        *   The default `user`, `password`, and `dbname` are used by the `Makefile` to connect to the Dockerized database. If you change them here, you **must** also change the corresponding `DB_USER`, `DB_PASSWORD` (if your `docker-compose.yml` uses it), and `DB_NAME` variables at the top of the `Makefile` or ensure your `docker-compose.yml` environment variables match. For this tutorial, using the defaults is fine.
    *   **`[exchange_api_keys]` section:**
        *   For fetching public market data like klines or symbol definitions from many exchanges (e.g., Binance), API keys might not be strictly necessary. However, for other operations or exchanges, they might be. Add your keys if you plan to use functionalities that require them.
    *   **Other sections:** Review other defaults like `default_start_date`, `default_kline_interval` as needed for your general usage, though we will override some of them with `make` variables in this tutorial.

## Step 2: Initialize the Database

This step will start the TimescaleDB Docker container and apply the database schema.

1.  **Run the setup command:**
   
```bash
make setup-db
```

This command performs two actions:
    *   `make up`: Starts the `timescaledb` service defined in your `docker-compose.yml` and waits for it to be ready.
    *   `make init-db`: Connects to the running database and executes the SQL commands in `db/schema.sql` to create tables, hypertables, continuous aggregates, etc.

2.  **Verify:**

You should see output indicating the Docker container is starting, a wait loop for the database to become ready, and then messages about the schema being applied. Look for "Database schema applied successfully."

## Step 3: Load Specific Symbol Definitions

Before fetching market data, we need to tell the system which trading symbols (instruments) we are interested in. We'll fetch their definitions from an exchange (e.g., Binance) and store them in our local database.

For this example, we will load definitions specifically for **BTC-USDT** and **ETC-USDT** as SPOT instruments on **Binance**.

1.  **Run the update-symbol-definitions command:**
   
```bash
make update-symbol-definitions \
    SYM_EXCHANGE=binance \
    SYM_QUOTE_ASSET_FILTER=USDT \
    SYM_SYMBOLS_LIST_FILTER="BTC-USDT,ETC-USDT" \
    SYM_INSTRUMENT_TYPE_FILTER=SPOT
```

* `SYM_EXCHANGE=binance`: Specifies that we want symbols from the Binance exchange.
* `SYM_QUOTE_ASSET_FILTER=USDT`: Ensures the quote asset is USDT.
* `SYM_SYMBOLS_LIST_FILTER="BTC-USDT,ETC-USDT"`: Crucially, this tells the script to only fetch definitions for this exact list of standard symbols.
* `SYM_INSTRUMENT_TYPE_FILTER=SPOT`: Ensures we are targeting SPOT instruments.

2.  **Verify:**

The script will connect to the Binance API, fetch symbol information, filter it based on your criteria (especially the `SYM_SYMBOLS_LIST_FILTER`), and then insert/update records in your `symbols` table. You'll see log output detailing its progress.

You should see something like:

```bash
2025-05-22 14:29:43 [INFO] data_ingestion.utils: Created new symbol: BTCUSDT (ExID: 1) -> StdBase: BTC, StdQuote: USDT, Type: SPOT. DB SymID: 1
2025-05-22 14:29:43 [INFO] data_ingestion.utils: Created new symbol: ETCUSDT (ExID: 1) -> StdBase: ETC, StdQuote: USDT, Type: SPOT. DB SymID: 2
```

## Step 4: Fetch Historical 1-Minute Kline Data

Now that we have our specific symbol definitions in the database, we can fetch their historical 1-minute kline (candlestick) data. The system defaults to fetching `1m` klines as this is considered the ground zero/source of truth data.

We'll fetch data starting from **January 1st, 2024**, for the symbols we loaded in the previous step.

1.  **Run the fetch-klines-from-db command:**

```bash
make fetch-klines-from-db \
    KLINE_FETCH_DB_EXCHANGE_NAME=binance \
    KLINE_FETCH_DB_QUOTE_ASSET=USDT \
    KLINE_FETCH_START_DATE="2024-01-01"
```

* `KLINE_FETCH_DB_EXCHANGE_NAME=binance`: Tells the script to look for symbols in your database that belong to the 'binance' exchange.
* `KLINE_FETCH_DB_QUOTE_ASSET=USDT`: Further filters those symbols to those quoted in USDT (matching what we loaded).
* `KLINE_FETCH_START_DATE="2024-01-01"`: Sets the earliest date from which to fetch klines. The script will automatically fetch up to the current date/time if no end date is specified.

2.  **Monitor:**

This step can take some time, depending on the date range and your internet connection. The script will output progress as it fetches data for each symbol. Your `data_ingestion.populate_db` script should handle API rate limits if they are encountered.

**Important Considerations:**
* **Data Volume & Time:** Be patient. Fetching historical data is intensive.
* **API Status Filter:** The `KLINE_FETCH_API_STATUS_FILTER` variable can be used if you only want to fetch klines for symbols that currently have a specific trading status on the exchange (e.g., "TRADING"). If not specified, it uses the default from your `config.ini` or script.
* **Max Workers:** `KLINE_FETCH_MAX_WORKERS` can be used to control parallel fetching for different symbols.

## Step 5: Refresh Data Aggregates & Materialized Views

After ingesting the raw 1-minute kline data, you need to refresh the TimescaleDB Continuous Aggregates (which create other intervals like 5m, 1h, 1d from the 1m data) and any custom Materialized Views (used for dashboard performance).

1.  **Refresh all data views:**

```bash
make refresh-all-data-views
```

This command is a shortcut that runs two sub-tasks:
    *   `make refresh-caggs`: Refreshes all defined Continuous Aggregates (e.g., `klines_5min`, `klines_1hour`).
    *   `make refresh-custom-mvs`: Refreshes materialized views like `symbol_1m_kline_stats`.

You should see output indicating that each CAGG and MV is being refreshed.

## Step 6: Basic Verification (Optional)

Once the data fetching and aggregate refresh are complete, you can inspect the database.

1.  **Open a database shell:**

```bash
make db-shell
```

This will open a `psql` command-line interface connected to your TimescaleDB.

2.  **Check the symbols table:**
   
```bash
SELECT ex.name as exchange_name, s.instrument_name as exch_symbol, s.base_asset || '-' || s.quote_asset || '-' || s.instrument_type as standard_symbol, count(*)
FROM symbols s
JOIN exchanges ex ON s.exchange_id = ex.exchange_id
GROUP BY ex.name, s.instrument_name, s.base_asset, s.quote_asset, s.instrument_type
ORDER BY ex.name, s.instrument_name;

-- To see specific symbols loaded (BTC-USDT, ETC-USDT):
SELECT s.*, ex.name as exchange_name FROM symbols s
JOIN exchanges ex ON s.exchange_id = ex.exchange_id
WHERE ex.name = 'binance'
  AND s.base_asset IN ('BTC', 'ETC') AND s.quote_asset = 'USDT' AND s.instrument_type = 'SPOT';
```

You should see entries for BTC-USDT and ETC-USDT.

```bash
 exchange_name | exch_symbol | standard_symbol | count 
---------------+-------------+-----------------+-------
 binance       | BTCUSDT     | BTC-USDT-SPOT   |     1
 binance       | ETCUSDT     | ETC-USDT-SPOT   |     1
(2 rows)

 symbol_id | exchange_id | instrument_name | base_asset | quote_asset | instrument_type | description |          created_at           |          updated_at           | exchange_name 
-----------+-------------+-----------------+------------+-------------+-----------------+-------------+-------------------------------+-------------------------------+---------------
         1 |           1 | BTCUSDT         | BTC        | USDT        | SPOT            |             | 2025-05-22 13:29:43.696261+00 | 2025-05-22 13:29:43.696261+00 | binance
         2 |           1 | ETCUSDT         | ETC        | USDT        | SPOT            |             | 2025-05-22 13:29:43.70102+00  | 2025-05-22 13:29:43.70102+00  | binance
(2 rows)
```

3.  **Check the klines table (`klines` which stores 1m data by default):**

```bash
-- Count raw 1-minute klines for BTC-USDT
SELECT count(*) FROM klines
WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE base_asset = 'BTC' AND quote_asset = 'USDT' AND instrument_type = 'SPOT' AND exchange_id = (SELECT exchange_id FROM exchanges WHERE name = 'binance') LIMIT 1)
  AND interval = '1m';

-- View some recent 1-minute klines for BTC-USDT
SELECT * FROM klines
WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE base_asset = 'BTC' AND quote_asset = 'USDT' AND instrument_type = 'SPOT' AND exchange_id = (SELECT exchange_id FROM exchanges WHERE name = 'binance') LIMIT 1)
  AND interval = '1m'
ORDER BY time DESC LIMIT 10;
```

You should see something like this:

```bash
 count  
--------
 730917
(1 row)

          time          | symbol_id | interval |       open_price       |       high_price       |       low_price        |      close_price       |       volume       |         close_time         |   quote_asset_volume    | number_of_trades | taker_buy_base_asset_volume | taker_buy_quote_asset_volume |          ingested_at          
------------------------+-----------+----------+------------------------+------------------------+------------------------+------------------------+--------------------+----------------------------+-------------------------+------------------+-----------------------------+------------------------------+-------------------------------
 2025-05-22 13:56:00+00 |         1 | 1m       | 111318.180000000000000 | 111318.190000000000000 | 111148.000000000000000 | 111173.920000000000000 | 19.801930000000000 | 2025-05-22 13:56:59.999+00 | 2202434.704092000000000 |             3902 |           3.814570000000000 |       424267.792566700000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:55:00+00 |         1 | 1m       | 111347.830000000000000 | 111354.000000000000000 | 111302.500000000000000 | 111318.180000000000000 |  9.289430000000000 | 2025-05-22 13:55:59.999+00 | 1034058.436094800000000 |             4091 |           4.648100000000000 |       517389.001157100000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:54:00+00 |         1 | 1m       | 111433.230000000000000 | 111435.330000000000000 | 111304.440000000000000 | 111347.830000000000000 | 10.733140000000000 | 2025-05-22 13:54:59.999+00 | 1195478.239737800000000 |             4699 |           2.336060000000000 |       260152.881497700000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:53:00+00 |         1 | 1m       | 111416.440000000000000 | 111474.980000000000000 | 111393.000000000000000 | 111433.230000000000000 | 22.612580000000000 | 2025-05-22 13:53:59.999+00 | 2519742.029419500000000 |             5106 |           9.356200000000000 |      1042565.916797700000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:52:00+00 |         1 | 1m       | 111241.910000000000000 | 111422.800000000000000 | 111240.000000000000000 | 111416.440000000000000 | 29.700400000000000 | 2025-05-22 13:52:59.999+00 | 3307260.147348400000000 |             9284 |          23.278790000000000 |      2592242.530278600000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:51:00+00 |         1 | 1m       | 111200.540000000000000 | 111247.830000000000000 | 111104.230000000000000 | 111241.900000000000000 | 19.835060000000000 | 2025-05-22 13:51:59.999+00 | 2205238.536454600000000 |            10116 |          11.895920000000000 |      1322568.643570800000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:50:00+00 |         1 | 1m       | 111219.990000000000000 | 111227.090000000000000 | 111045.520000000000000 | 111200.540000000000000 | 33.436560000000000 | 2025-05-22 13:50:59.999+00 | 3716677.467590400000000 |             9793 |          16.675410000000000 |      1853688.380551500000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:49:00+00 |         1 | 1m       | 111076.660000000000000 | 111220.000000000000000 | 110980.000000000000000 | 111220.000000000000000 | 35.774920000000000 | 2025-05-22 13:49:59.999+00 | 3975013.629040000000000 |             8765 |          23.909820000000000 |      2657214.579298300000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:48:00+00 |         1 | 1m       | 111040.000000000000000 | 111307.690000000000000 | 111039.990000000000000 | 111076.660000000000000 | 34.867500000000000 | 2025-05-22 13:48:59.999+00 | 3875362.539591000000000 |            10636 |          20.787530000000000 |      2310119.728334400000000 | 2025-05-22 13:56:29.731611+00
 2025-05-22 13:47:00+00 |         1 | 1m       | 111082.230000000000000 | 111106.920000000000000 | 110964.210000000000000 | 111040.000000000000000 | 37.646670000000000 | 2025-05-22 13:47:59.999+00 | 4179615.822181000000000 |            10774 |          14.187020000000000 |      1575009.480447400000000 | 2025-05-22 13:56:29.731611+00
(10 rows)
```

4.  **Check a Continuous Aggregate (e.g., `klines_1day`):**

```bash
-- Count 1-day aggregated klines for BTC-USDT
SELECT COUNT(*) FROM klines_1day
WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE base_asset = 'BTC' AND quote_asset = 'USDT' AND instrument_type = 'SPOT' AND exchange_id = (SELECT exchange_id FROM exchanges WHERE name = 'binance') LIMIT 1);

-- View some recent 1-day aggregated klines for BTC-USDT
SELECT * FROM klines_1day
WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE base_asset = 'BTC' AND quote_asset = 'USDT' AND instrument_type = 'SPOT' AND exchange_id = (SELECT exchange_id FROM exchanges WHERE name = 'binance') LIMIT 1)
ORDER BY bucket_time DESC LIMIT 10;
```

Output:

```bash
 count 
-------
   508
(1 row)

      bucket_time       | symbol_id |          open          |          high          |          low           |         close          |        volume         |         close_time         |     quote_asset_volume     | trades  | taker_buy_base_asset_volume | taker_buy_quote_asset_volume 
------------------------+-----------+------------------------+------------------------+------------------------+------------------------+-----------------------+----------------------------+----------------------------+---------+-----------------------------+------------------------------
 2025-05-22 00:00:00+00 |         1 | 109643.990000000000000 | 111880.000000000000000 | 109177.370000000000000 | 111173.920000000000000 | 23171.938740000000000 | 2025-05-22 13:56:59.999+00 | 2568361867.506589600000000 | 3978732 |       12381.820520000000000 |   1372492033.808500100000000
 2025-05-21 00:00:00+00 |         1 | 106850.000000000000000 | 110797.380000000000000 | 106100.010000000000000 | 109643.990000000000000 | 45531.040345000000000 | 2025-05-21 23:59:59.999+00 | 4914709514.320026800000000 | 6835279 |       23661.649395000000000 |   2555528481.098725800000000
 2025-05-20 00:00:00+00 |         1 | 105573.730000000000000 | 107320.000000000000000 | 104184.720000000000000 | 106849.990000000000000 | 23705.482750000000000 | 2025-05-20 23:59:59.999+00 | 2509102444.669598200000000 | 3878351 |       12350.462750000000000 |   1307273652.389523200000000
 2025-05-19 00:00:00+00 |         1 | 106454.270000000000000 | 107108.620000000000000 | 102000.000000000000000 | 105573.740000000000000 | 30260.035240000000000 | 2025-05-19 23:59:59.999+00 | 3150324504.595451500000000 | 4881371 |       14208.309000000000000 |   1480452895.084156200000000
 2025-05-18 00:00:00+00 |         1 | 103126.650000000000000 | 106660.000000000000000 | 103105.090000000000000 | 106454.260000000000000 | 21599.987260000000000 | 2025-05-18 23:59:59.999+00 | 2262214972.074938400000000 | 3193300 |       11664.899040000000000 |   1221914552.055201200000000
 2025-05-17 00:00:00+00 |         1 | 103463.900000000000000 | 103709.860000000000000 | 102612.500000000000000 | 103126.650000000000000 | 11250.896220000000000 | 2025-05-17 23:59:59.999+00 | 1160418362.950618100000000 | 2568597 |        5335.115250000000000 |    550379728.733850100000000
 2025-05-16 00:00:00+00 |         1 | 103763.710000000000000 | 104550.330000000000000 | 103100.490000000000000 | 103463.900000000000000 | 15683.880240000000000 | 2025-05-16 23:59:59.999+00 | 1628832372.324148100000000 | 3237442 |        7555.158010000000000 |    784850382.194274700000000
 2025-05-15 00:00:00+00 |         1 | 103507.830000000000000 | 104192.700000000000000 | 101383.070000000000000 | 103763.710000000000000 | 17998.986040000000000 | 2025-05-15 23:59:59.999+00 | 1850183474.529203600000000 | 4139477 |        8475.180360000000000 |    871244345.564136000000000
 2025-05-14 00:00:00+00 |         1 | 104103.720000000000000 | 104356.950000000000000 | 102602.050000000000000 | 103507.820000000000000 | 16452.908100000000000 | 2025-05-14 23:59:59.999+00 | 1704708279.388410900000000 | 3025221 |        8026.033330000000000 |    831783471.654361000000000
 2025-05-13 00:00:00+00 |         1 | 102791.320000000000000 | 104976.250000000000000 | 101429.700000000000000 | 104103.720000000000000 | 21253.424090000000000 | 2025-05-13 23:59:59.999+00 | 2197244118.152366900000000 | 3703531 |       10705.978450000000000 |   1106609664.707795000000000
(10 rows)
```

*(Note: `bucket_time` is the time column for continuous aggregates).*

5.  **Exit psql:**
    Type `\q` and press Enter.

## Next Steps

You have now initialized your database, populated it with specific symbol definitions, fetched their 1-minute historical kline data, and refreshed data aggregates. From here, you can:

*  `make calculate-features FEATURE_SET_VERSION=v1_test`: To generate trading features from the kline data.
*  `make collect-orderbook-snapshots`: To collect live order book data for a specific symbol.
*  `make webui`: To start the experimental web interface and visualize the data.
*  Run `make help` to see all available targets and their descriptions.

## Troubleshooting

* **Docker Issues:** Ensure Docker Desktop (or Docker daemon on Linux) is running. If `make up` fails, check Docker logs with `make logs`.
* **Configuration Errors:** Double-check `config/config.ini`. Errors here can prevent scripts from running correctly.
* **API Rate Limits:** If fetching data takes too long or errors out, you might be hitting exchange API rate limits. The scripts should ideally handle some of this, but you might need to adjust `KLINE_FETCH_MAX_WORKERS` or `KLINE_FETCH_WORKER_DELAY` or fetch data in smaller batches/time ranges.
* **Schema Issues:** If `make init-db` fails, there might be an issue with `db/schema.sql` or the database connection.

Happy Data Ingesting!