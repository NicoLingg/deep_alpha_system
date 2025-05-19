# Deep Alpha System

**Deep Alpha System** is a comprehensive framework for collecting, storing, analyzing financial market data (initially from Binance), and providing a foundation for developing and training machine learning models for trading strategies.

The system is designed to:
1.  Ingest high-resolution kline (candlestick) data (e.g., 1-minute) as the source of truth.
2.  Periodically collect order book snapshots.
3.  Store this data efficiently in a TimescaleDB database, leveraging its time-series capabilities.
4.  Automatically create aggregated views (e.g., 5-min, 1-hour, 1-day klines) using TimescaleDB Continuous Aggregates.
5.  Provide a basic web UI for data visualization and inspection.
6.  Offer a structured environment for feature engineering and training ML models (future goal).

## Features

*   **Automated Data Ingestion:**
    *   Historical 1-minute kline data downloader for specified symbols and date ranges.
    *   Continuous polling script for collecting order book snapshots at configurable intervals.
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
    *   Simplified commands for setting up the database, ingesting data, running the web UI, and managing the system.
*   **Dockerized Database:**
    *   TimescaleDB runs in a Docker container for easy setup and portability.
*   **Modular Python Code:**
    *   Data ingestion scripts are organized into a Python package.
    *   Shared utilities for database connections and client initializations.
*   **Foundation for ML:**
    *   The structured data storage and clear data ingestion pipeline provide a solid base for building ML models (ML components to be implemented).

## Project Structure (Overview)

*   `config/`: Contains configuration files (e.g., `config.ini`).
*   `data_ingestion/`: Python scripts for fetching data from Binance and storing it.
*   `db/`: SQL schema definitions (`schema.sql`).
*   `web_ui/`: Flask application for the web interface, including templates and static files.
*   `Makefile`: For automating common tasks like setup, data ingestion, and running services.
*   `docker-compose.yml`: Defines the TimescaleDB service.
*   `requirements.txt`: Python dependencies.

## Prerequisites

*   **Docker and Docker Compose:** For running the TimescaleDB database.
*   **Python 3.8+:** For running the data ingestion scripts and web UI.
*   **Make:** For using the Makefile commands.
*   **Conda (Recommended for environment management):** While optional (you can use `venv`), Conda is recommended.
*   **Binance Account (Optional):** API keys are optional for public data but might provide higher rate limits if used.

## Setup and Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/NicoLingg/deep_alpha_system.git
    cd deep_alpha_system
    ```

2.  **Set up Python Environment (Conda Recommended):**
    It's highly recommended to use a virtual environment to manage project dependencies.

    **Using Conda (Recommended):**
    If you don't have Conda, download and install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/products/distribution).
    ```bash
    # Create a new Conda environment (e.g., named 'deep_alpha_env' with Python 3.9)
    conda create --name deep_alpha_env python=3.9 -y

    # Activate the environment
    conda activate deep_alpha_env
    ```
    You'll need to activate this environment (`conda activate deep_alpha_env`) every time you work on the project in a new terminal session.

    **Using `venv` (Alternative):**
    If you prefer not to use Conda, you can use Python's built-in `venv` module:
    ```bash
    # Create a virtual environment (e.g., named 'venv')
    python3 -m venv venv

    # Activate the environment
    # On macOS and Linux:
    source venv/bin/activate
    # On Windows:
    # .\venv\Scripts\activate
    ```

3.  **Install Python Dependencies:**
    Ensure your virtual environment (Conda or venv) is activated.
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Passwords and API Keys:**
    *   It's good practice to create `config/config.ini` from an example. If `config/config.ini.example` exists:
        ```bash
        cp config/config.ini.example config/config.ini
        ```
        Otherwise, ensure `config/config.ini` is present or create it based on the required structure.
    *   Open `config/config.ini` and update the `password` under the `[database]` section. **This password MUST match** the `POSTGRES_PASSWORD` in `docker-compose.yml`.
    *   Optionally, add your Binance API `api_key` and `api_secret` under the `[binance]` section if you have them.
    *   Open `docker-compose.yml` and update `POSTGRES_PASSWORD` to match the password you set in `config.ini`.
    *   **IMPORTANT SECURITY NOTE:** Do not commit your actual `config.ini` with real passwords or API keys to a public repository. The file `config/config.ini` should ideally be listed in your `.gitignore` file (if it isn't already meant to be a template).

5.  **Start the Database Container:**
    This command will download the TimescaleDB image (if not already present) and start the database container in the background.
    ```bash
    make up
    ```
    Wait for the message "Database is ready." If you encounter issues, check the logs: `make logs`.

6.  **Initialize the Database Schema:**
    This applies the schema defined in `db/schema.sql` to the running database, creating tables, hypertables, and continuous aggregates.
    ```bash
    make init-db
    ```

## Usage

The `Makefile` provides convenient targets for managing the system. Ensure your Python environment (Conda or venv) is activated before running `make` targets that execute Python scripts.

### Data Ingestion

**1. Fetching Historical 1-Minute Kline Data:**
   This script (`data_ingestion/kline_ingestor.py`) downloads 1-minute kline data which serves as the source for all aggregated views.

   *   **Fetch for a single symbol (default: BTCUSDT, from 1 day ago):**
        ```bash
        make fetch-klines
        ```
   *   **Fetch for a specific symbol and start date:**
        ```bash
        make fetch-klines SYMBOL=ETHUSDT START_DATE="2023-01-01"
        ```
   *   **Fetch for a specific date range:**
        ```bash
        make fetch-klines SYMBOL=SOLUSDT START_DATE="2023-06-01" END_DATE="2023-07-01"
        ```

   *   **Bulk Fetch for Multiple Symbols:**
        1.  (Optional) Identify liquid symbols:
            ```bash
            make get-liquid-symbols TOP_N=10 QUOTE_ASSET=USDT MIN_VOLUME=5000000
            ```
        2.  Edit the `BULK_SYMBOLS` variable in the `Makefile` with the list of symbols you want.
        3.  Run the bulk fetch (uses `START_DATE` and optional `END_DATE` from Makefile or command line):
            ```bash
            make fetch-klines-bulk START_DATE="2024-01-01"
            ```
            This will iterate through the `BULK_SYMBOLS` list, fetching data for each with a small delay in between.

**2. Collecting Order Book Snapshots:**
   This script (`data_ingestion/orderbook_ingestor.py`) runs continuously to poll and store order book snapshots at regular intervals.

   *   **Start collecting for a symbol (default: BTCUSDT, every 60s, 100 levels):**
        ```bash
        make collect-orderbook-snapshots
        ```
   *   **Collect for a specific symbol with custom interval and limit:**
        ```bash
        make collect-orderbook-snapshots SNAPSHOT_SYMBOL=ADAUSDT SNAPSHOT_INTERVAL_SECONDS=30 SNAPSHOT_LIMIT=50
        ```
   Press `Ctrl+C` to stop the collector script gracefully.

**3. Refreshing Continuous Aggregates:**
   While policies refresh aggregates automatically, you can trigger a manual refresh:
   ```bash
   make refresh-aggregates
   ```

## Web UI

To start the web UI:
```bash
make webui
```
Once running, you can access the UI in your browser, typically at `http://localhost:5001` or `http://0.0.0.0:5001`.

### Database Management

Common Makefile targets for managing the database:

*   **Access psql shell:**
    ```bash
    make db-shell
    ```
*   **Stop database container:**
    ```bash
    make down
    ```
*   **View database logs:**
    ```bash
    make logs
    ```
*   **Clean database data (WARNING: destructive):**
    This command will stop the database container and remove all its persisted data.
    ```bash
    make clean-db-data
    ```

## Troubleshooting

*   **Database Connection Issues:**
    *   Ensure the TimescaleDB Docker container is running: `docker ps`
    *   Verify that `POSTGRES_PASSWORD` in `docker-compose.yml` matches the `password` in `config/config.ini`.
    *   Check that the `host` and `port` in `config/config.ini` (e.g., `host = localhost`, `port = 5433`) match the host-side port mapping in `docker-compose.yml` (e.g., `ports: - "5433:5432"`).

*   **Port Conflicts:**
    *   If port `5433` (for the database) or `5001` (for the web UI) is already in use on your system, you'll need to change it.
    *   **For the database:** Modify the host-side port in `docker-compose.yml` (e.g., change `"5433:5432"` to `"5434:5432"`) and update the `port` in `config/config.ini` accordingly (e.g., to `5434`).
    *   **For the web UI:** Change the port in `web_ui/webui.py` (where `app.run` is called) and/or in the `Makefile` if it's hardcoded there for the `webui` target.

*   **Python `ModuleNotFoundError`:**
    *   Make sure your Python virtual environment (Conda or `venv`) is activated.
    *   Ensure you have installed all dependencies: `pip install -r requirements.txt`
