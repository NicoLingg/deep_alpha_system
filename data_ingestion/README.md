# Data Ingestion Pipeline

This directory (`data_ingestion/`) contains the core components and scripts for fetching, standardizing, and storing market data (klines and order book snapshots) from cryptocurrency exchanges. It's designed to be modular and configurable, feeding data into a TimescaleDB database.

## Core Functionality

The pipeline revolves around a few key scripts and a standardized approach to handling exchange data:

1.  **Symbol Management (`manage_symbols.py`):**
    *   **Purpose:** To discover all tradable instruments on a given exchange and store their standardized representation in the local `symbols` database table. This table acts as a central catalog.
    *   **Process:**
        *   Uses an exchange-specific "adapter" (see below) to call the exchange's API endpoint that lists all symbols/instruments.
        *   For each instrument returned by the exchange:
            *   The adapter extracts the exchange's native symbol name (e.g., "BTCUSDT"), its base asset (e.g., "BTC"), and quote asset (e.g., "USDT").
            *   **Standardization:** The adapter then converts these into a standard internal representation. This includes ensuring consistent naming for base and quote assets (e.g., "BTC" is always "BTC", "USDT" is "USDT") and determining a standard `instrument_type` (`SPOT`, `PERP`, `FUTURE_YYMMDD`) using `SymbolRepresentation`. 
        *   The script then inserts or updates this information (exchange-specific name, standard base, standard quote, standard type) into the `symbols` table.

2.  **Kline Ingestion (`populate_db.py` using `kline_ingestor.py`):**
    *   **Purpose:** To fetch historical kline (OHLCV) data for symbols that are *already defined* in the local `symbols` table.
    *   **Process:**
        *   Queries the local `symbols` table based on user-provided filters (e.g., by exchange, by standard quote asset, or by a specific list of standard symbol strings like "BTC-USDT-SPOT").
        *   The `get_target_symbols_from_local_db` function selects appropriate database entries. If multiple exchange instruments might map to the same requested standard symbol (due to the adapter's standardization logic), `DISTINCT ON` is used to pick a single, most representative entry.
        *   For each selected symbol, an asynchronous worker task is launched:
            *   It may first check the symbol's live trading status via the adapter.
            *   It calls `fetch_and_store_historical_klines` (from `kline_ingestor.py`).
            *   `fetch_and_store_historical_klines`:
                *   Determines the required date ranges by comparing requested dates with existing data in the DB.
                *   For each data segment needed, it enters a **pagination loop**:
                    *   Calls the exchange adapter's `fetch_klines` method, providing the *standard symbol string* (e.g., "BTC-USDT-SPOT"), interval, and date range.
                    *   The adapter's `fetch_klines` method normalizes the standard symbol to the exchange-specific format, makes paginated API calls, and transforms the raw API response into a standardized Pandas DataFrame (with specific columns, `Decimal` types, and UTC timestamps).
                    *   The `fetch_and_store_historical_klines` loop explicitly handles further pagination for "fetch to latest" scenarios or very long date ranges.
                *   The fetched DataFrame batch is then passed to `store_klines_batch`.
                *   `store_klines_batch`: Inserts/updates the kline data into the `klines` database table.

3.  **Order Book Ingestion (`orderbook_ingestor.py`):**
    *   **Purpose:** To periodically fetch and store full order book snapshots.
    *   **Process:**
        *   For a user-specified standard symbol, it runs a polling loop.
        *   In each iteration, it calls the exchange adapter's `fetch_orderbook_snapshot` method.
        *   The adapter fetches the snapshot, converts prices/sizes to `Decimal`, and returns a standardized dictionary including `lastUpdateId` and `exchange_ts` (exchange's event timestamp, if available; often `None` for spot markets).
        *   The script stores this snapshot in the `order_book_snapshots` table. The `retrieved_at` column (partitioning key) uses `exchange_ts` if provided, otherwise defaults to the ingestion time.

## Key Abstractions

*   **`exchanges/base_interface.py:ExchangeInterface`**: An abstract base class defining the contract all exchange adapters must implement.
*   **`exchanges/symbol_representation.py:SymbolRepresentation`**: A class to parse and generate standardized internal symbol strings (e.g., "BTC-USDT-SPOT").
*   **Exchange Adapters (e.g., `exchanges/binance_adapter.py:BinanceAdapter`)**: Concrete implementations of `ExchangeInterface`. They handle:
    *   API client initialization.
    *   Fetching raw data.
    *   Translating between the exchange's data formats/symbol names and the system's internal standards (base/quote assets, instrument types).
    *   Managing an internal cache of symbol details for efficient symbol normalization.

## Configuration

Refer to `config/config.ini` and the main project README. Exchange-specific configurations can be placed in sections like `[binance]`.

## Extending Functionality

### Adding a New Exchange

To add support for a new exchange (e.g., "Kraken"):

1.  **Create Adapter File:** In `data_ingestion/exchanges/`, create `kraken_adapter.py`.
2.  **Implement `KrakenAdapter(ExchangeInterface)`:**
    *   Implement all abstract methods from `ExchangeInterface`.
    *   **Key Responsibilities for the Adapter:**
        *   **Symbol Normalization:** Convert Kraken's native pair names into your system's standard base asset, quote asset, and instrument type using `SymbolRepresentation`. Decide how Kraken's asset names (e.g., "XBT", "ZUSD") should be represented as your internal standard assets (e.g., "BTC", "USD"). This might involve creating a mapping dictionary within the adapter if Kraken's names differ from your desired standard.
        *   **Data Transformation:** Ensure klines are returned as the standard DataFrame and order books as the standard dictionary, with `Decimal` types and UTC timestamps.
        *   **Pagination:** Implement logic within the adapter's `fetch_klines` if the Kraken API/client library doesn't automatically handle fetching all data for a large date range.
        *   **Error Handling:** Manage API-specific exceptions.
3.  **Register Adapter:**
    *   In `data_ingestion/utils.py`, add `KrakenAdapter` to `_get_adapter_map()`.
    *   In `data_ingestion/exchanges/__init__.py`, import and add `KrakenAdapter` to `__all__`.
4.  **Configuration:** Add a `[kraken]` section to `config/config.ini` if needed.
5.  **Test Thoroughly.**