# Deep Alpha System

<p align="center">
  <img src="assets/logo.png" alt="Deep Alpha System Logo" width="250">
</p>

**Deep Alpha System** serves as an exploratory environment for those looking to delve into machine learning applications for financial markets and gain hands-on experience with the components of a quantitative trading pipeline. It provides a practical framework for collecting, storing, and analyzing market data (initially from Binance) as a basis for developing and evaluating trading strategies.

The system is designed to:
1.  Define and manage a local list of relevant financial instrument symbols.
2.  Ingest high-resolution kline (candlestick) data (e.g., 1-minute) for these symbols, serving as the source of truth.
3.  Periodically collect order book snapshots for selected symbols.
4.  Store this data efficiently in a TimescaleDB database, leveraging its time-series capabilities.
5.  Automatically create aggregated views (e.g., 5-min, 1-hour, 1-day klines) using TimescaleDB Continuous Aggregates.
6.  Provide a basic web UI for data visualization and inspection.
7.  Offer a structured environment for feature engineering and training ML models (future goal).

**Note:** This project is currently under active development. Core data ingestion and storage functionalities are operational. Feature engineering and machine learning model integration are planned future additions.

## Features

*   **Staged Data Ingestion Workflow:**
    *   **Stage 1 (Symbol Definition):** Updates the local database's `symbols` table from the Binance API based on configurable filters (status, permissions, quote asset).
    *   **Stage 2 (Kline Ingestion):** Fetches historical 1-minute kline data for symbols *already defined* in the local database, with support for concurrent fetching and smart range determination to minimize redundant downloads.
*   **Order Book Collection:** Continuous polling script for collecting order book snapshots at configurable intervals.
*   **Robust Data Storage:**
    *   Utilizes PostgreSQL with the TimescaleDB extension for optimized time-series data handling.
    *   Schema includes tables for exchanges, symbols, raw klines, and order book snapshots.
    *   Hypertables for efficient querying and data management.
*   **Continuous Aggregates:**
    *   Pre-computed aggregated kline views (5m, 10m, 15m, 30m, 1h, 6h, 1d) derived from 1-minute data.
*   **Data Exploration & Visualization:**
    *   A Flask-based web UI to view kline data (raw and aggregated) and order book snapshots.
    *   Symbol statistics (data counts, date ranges) available in the UI.
*   **Orchestration with Makefile:**
    *   Simplified commands for setting up the database, managing symbols, ingesting data, running the web UI, and other maintenance tasks.
*   **Dockerized Database:**
    *   TimescaleDB runs in a Docker container for easy setup and portability.

## Project Structure (Overview)

*   `config/`: Contains configuration files (e.g., `config.ini`).
*   `data_ingestion/`: Python scripts for managing symbols and fetching data from Binance.
    *   `manage_symbols.py`: Stage 1 - Updates local symbol definitions.
    *   `populate_db.py`: Stage 2 - Fetches klines for symbols in the local DB.
    *   `kline_ingestor.py`: Core kline fetching and storage logic (used by `populate_db.py` and for single fetches).
    *   `orderbook_ingestor.py`: Collects order book data.
    *   `utils.py`: Shared utility functions.
    *   `symbol_utils.py`: Utilities like finding top liquid symbols.
*   `db/`: SQL schema definitions (`schema.sql`).
*   `web_ui/`: Flask application for the web interface.
*   `Makefile`: For automating common tasks.
*   `docker-compose.yml`: Defines the TimescaleDB service.
*   `requirements.txt`: Python dependencies.


## Prerequisites

*   **Docker and Docker Compose:** For running the TimescaleDB database.
*   **Python 3.8+:** For running the data ingestion scripts and web UI.
*   **Make:** For using the Makefile commands.
*   **Conda (Recommended for environment management):** While optional (you can use `venv` too)
*   **Binance Account (Optional):** API keys are not strictly needed for public data but might provide higher rate limits if used.

## Setup and Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/NicoLingg/deep_alpha_system.git
    cd deep_alpha_system
    ```

2.  **Set up Python Environment (Conda Recommended):**
    If you don't have Conda, download and install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/products/distribution).
    ```bash
    # Create a new Conda environment (e.g., named 'deep_alpha_env' with Python 3.9 or higher)
    conda create --name deep_alpha_env python=3.9 -y
    conda activate deep_alpha_env
    ```
    Activate this environment (`conda activate deep_alpha_env`) every time you work on the project.

3.  **Install Python Dependencies:**
    Ensure your virtual environment is activated.
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Passwords and API Keys:**
    *   Copy `config/config.ini.example` to `config/config.ini` if an example file exists, or ensure `config/config.ini` is present.
    *   Open `config/config.ini`:
        *   Update `password` under `[database]`. **This MUST match** `POSTGRES_PASSWORD` in `docker-compose.yml`.
        *   Update `username` and `password` under `[webui_auth]` if you want to protect the Web UI.
        *   Optionally, add your Binance `api_key` and `api_secret` under `[binance]`.
    *   Open `docker-compose.yml` and update `POSTGRES_PASSWORD` to match the database password you set in `config.ini`.
    *   **SECURITY NOTE:** Do not commit `config.ini` with real credentials to a public repository. It should ideally be in `.gitignore` (unless it's intended as a template without secrets).

5.  **Start Database & Initialize Schema (Combined Target):**
    This command starts the Dockerized TimescaleDB and applies the database schema.
    ```bash
    make setup-db
    ```
    Wait for "Database is ready." and "Database schema applied successfully." If issues occur, check logs: `make logs`.

## Usage

The `Makefile` provides convenient targets. Ensure your Python environment is activated. Date parameters (`START_DATE`, `END_DATE`) generally expect specific formats like "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS".

### Core Data Ingestion Workflow

**Stage 1: Update Symbol Definitions**
This populates/updates your local `symbols` table from the Binance API. Symbols are filtered based on `config.ini` settings under `[symbol_management]` or can be overridden by Makefile variables.
```bash
make update-symbol-definitions
```
To customize filters (see `Makefile` help for variable names):
```bash
make update-symbol-definitions SYM_STATUSES="TRADING,END_OF_TRADING" SYM_PERMISSIONS_HINT="SPOT" SYM_QUOTE_ASSET="USDT"
```
*   `SYM_STATUSES`: Comma-separated list (e.g., "TRADING,END_OF_TRADING").
*   `SYM_PERMISSIONS_HINT`: Comma-separated list (e.g., "SPOT").
*   `SYM_QUOTE_ASSET`: Specific quote asset (e.g., "USDT", "BTC").

**Stage 2: Fetch Klines for Defined Symbols**
This fetches kline data for symbols that are *already in your local database* (populated by Stage 1).
```bash
make fetch-klines-from-db
```
This uses defaults from `config.ini` (e.g., `default_start_date`, `default_db_quote_asset_filter`, `default_kline_fetch_api_status_filter`).

To customize, for example, to fetch data for all USDT pairs from 2022-01-01, attempting to get klines regardless of their current live API status (useful for `END_OF_TRADING` symbols):
```bash
make fetch-klines-from-db KLINE_FETCH_DB_QUOTE_ASSET="USDT" KLINE_FETCH_START_DATE="2022-01-01" KLINE_FETCH_API_STATUS_FILTER=NONE
```
*   See `make help` for all `KLINE_FETCH_*` variables.

### Other Data Targets

**Fetch Klines for a Single Symbol (Legacy/Direct)**
Useful for quick tests or fetching specific symbols not covered by bulk logic.
```bash
# Uses default START_DATE from Makefile (e.g., "2024-01-01")
make fetch-single-kline SYMBOL=BTCUSDT

# Specific date range
make fetch-single-kline SYMBOL=ETHUSDT START_DATE="2023-01-01" END_DATE="2023-01-05"

# Fetch to latest
make fetch-single-kline SYMBOL=ADAUSDT START_DATE="2024-05-01"
```

**Collect Order Book Snapshots**
Runs continuously. Press `Ctrl+C` to stop.
```bash
make collect-orderbook-snapshots SNAPSHOT_SYMBOL=BTCUSDT SNAPSHOT_INTERVAL_SECONDS=30
```

**Refresh Continuous Aggregates**
Manually trigger updates for TimescaleDB materialized views.
```bash
make refresh-aggregates
```

### Web UI

Start the Flask web interface:
```bash
make webui
```
Access in your browser, typically at `http://localhost:5004` (or as configured in `web_ui/webui.py`).

It will look like this:
![alt text](assets/data_viewer.gif)

### Database Management

*   **Access psql shell:** `make db-shell`
*   **Stop database container:** `make down`
*   **View database logs:** `make logs`
*   **Clean database data (WARNING: destructive):** `make clean-db-data` (stops container and removes data volume)

## Troubleshooting

*   **Database Connection Issues:**
    *   Ensure Docker container is running (`docker ps`).
    *   Verify passwords match between `config.ini` and `docker-compose.yml`.
    *   Confirm host/port in `config.ini` matches Docker port mapping.
*   **Port Conflicts:**
    *   If port `5433` (DB) or `5004` (WebUI) is in use, change the host-side port in `docker-compose.yml` (e.g., `"5435:5432"`) and update `config.ini` (`port = 5435`). For WebUI, change in `web_ui/webui.py`.
*   **Python `ModuleNotFoundError`:**
    *   Ensure your Python virtual environment is activated.
    *   Run `pip install -r requirements.txt`.
*   **Date Parsing Errors for Kline Ingestion:**
    *   Ensure `START_DATE` and `END_DATE` provided to `make fetch-single-kline` or `make fetch-klines-from-db` (or set in `config.ini`) use the format "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS". Relative dates like "1 day ago" are no longer supported for these parameters.

