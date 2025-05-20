# --- Configuration ---
PYTHON = python3
DOCKER_COMPOSE = docker-compose
CONFIG_FILE_PATH = config/config.ini
SCHEMA_FILE_PATH = db/schema.sql
CONTAINER_DB_NAME = deep_alpha_timescaledb
DB_USER = deep_alpha_user
DB_NAME = deep_alpha_data_db
# Default symbol and interval for single kline fetch (legacy)
DEFAULT_SYMBOL = BTCUSDT
DEFAULT_KLINE_INTERVAL = 1m
DEFAULT_START_DATE = 2025-01-01


# --- Vars for Stage 1: Update Symbol Definitions ---
SYM_STATUSES = # Uses config: symbol_management.default_include_statuses
SYM_PERMISSIONS = # Uses config: symbol_management.default_include_permissions
SYM_QUOTE_ASSET = # Optional quote asset filter for definition stage

# --- Vars for Stage 2: Fetch Klines from DB ---
KLINE_FETCH_DB_QUOTE_ASSET = # Optional: filter DB symbols by quote_asset
KLINE_FETCH_DB_INSTRUMENT_LIKE = # Optional: SQL LIKE for instrument_name from DB
KLINE_FETCH_DB_SYMBOLS_LIST = # Optional: specific list from DB "SYM1,SYM2"
KLINE_FETCH_START_DATE = # Uses config: ingestion.default_start_date
KLINE_FETCH_END_DATE = # Optional: for kline end date
KLINE_FETCH_INTERVAL = # Uses config: ingestion.default_kline_interval
KLINE_FETCH_API_STATUS_FILTER = # Uses config: symbol_management.default_kline_fetch_api_status_filter
KLINE_FETCH_MAX_WORKERS = # Uses config: ingestion.max_workers
KLINE_FETCH_WORKER_DELAY = # Uses config: ingestion.worker_symbol_delay

.PHONY: all help build up down logs restart status db-shell init-db setup-db \
        update-symbol-definitions fetch-klines-from-db fetch-single-kline \
        collect-orderbook-snapshots refresh-aggregates webui clean clean-db-data

all: help

help:
	@echo "Deep Alpha System Makefile"
	@echo ""
	@echo "Core Workflow Targets (Staged Approach):"
	@echo "  setup-db                    Start Docker DB and initialize schema."
	@echo "  update-symbol-definitions   Stage 1: Update local 'symbols' table from Binance API."
	@echo "                              Vars: SYM_STATUSES, SYM_PERMISSIONS, SYM_QUOTE_ASSET"
	@echo "  fetch-klines-from-db        Stage 2: Fetch klines for symbols ALREADY IN LOCAL DB."
	@echo "                              Vars: KLINE_FETCH_DB_QUOTE_ASSET, KLINE_FETCH_DB_INSTRUMENT_LIKE, KLINE_FETCH_DB_SYMBOLS_LIST,"
	@echo "                                    KLINE_FETCH_START_DATE, KLINE_FETCH_END_DATE, KLINE_FETCH_INTERVAL,"
	@echo "                                    KLINE_FETCH_API_STATUS_FILTER, KLINE_FETCH_MAX_WORKERS, KLINE_FETCH_WORKER_DELAY"
	@echo ""
	@echo "Other Data Targets:"
	@echo "  fetch-single-kline          (Legacy) Fetch klines for a single specified symbol."
	@echo "                              Vars: SYMBOL, START_DATE, END_DATE, INTERVAL, BASE_ASSET, QUOTE_ASSET"
	@echo "  collect-orderbook-snapshots Start collecting order book snapshots."
	@echo "  refresh-aggregates          Manually refresh TimescaleDB continuous aggregates."
	@echo ""
	@echo "Service Targets:"
	@echo "  up                          Start database container."
	@echo "  init-db                     Initialize database schema (run after 'up')."
	@echo "  webui                       Start the Flask web UI."
	@echo "  down                        Stop and remove database container."
	@echo "  logs                        Show logs for the database container."
	@echo "  restart                     Restart the database container."
	@echo "  status                      Show status of Docker services."
	@echo "  db-shell                    Open a psql shell to the database."
	@echo ""
	@echo "Cleanup Targets:"
	@echo "  clean                       Remove Python bytecode and __pycache__."
	@echo "  clean-db-data               WARNING: Stops container and removes database data volume."
	@echo ""
	@echo "Example Stage 1: make update-symbol-definitions SYM_STATUSES=\"TRADING,BREAK\" SYM_PERMISSIONS=\"SPOT\""
	@echo "Example Stage 2: make fetch-klines-from-db KLINE_FETCH_DB_QUOTE_ASSET=USDT KLINE_FETCH_START_DATE=\"2022-01-01\""

# --- Core Workflow Targets ---
setup-db: up init-db

update-symbol-definitions: up
	@echo "Starting Stage 1: Updating symbol definitions in local DB from Binance API..."
	$(PYTHON) -m data_ingestion.manage_symbols --config $(CONFIG_FILE_PATH) \
		$(if $(SYM_STATUSES),--include-statuses "$(SYM_STATUSES)",) \
		$(if $(SYM_PERMISSIONS_HINT),--include-permissions-hint "$(SYM_PERMISSIONS_HINT)",) \
		$(if $(SYM_QUOTE_ASSET),--quote-asset-filter $(SYM_QUOTE_ASSET),)

fetch-klines-from-db: up
	@echo "Starting Stage 2: Fetching klines for symbols defined in local DB..."
	$(PYTHON) -m data_ingestion.populate_db --config $(CONFIG_FILE_PATH) \
		$(if $(KLINE_FETCH_DB_QUOTE_ASSET),--db-quote-asset $(KLINE_FETCH_DB_QUOTE_ASSET),) \
		$(if $(KLINE_FETCH_DB_INSTRUMENT_LIKE),--db-instrument-like "$(KLINE_FETCH_DB_INSTRUMENT_LIKE)",) \
		$(if $(KLINE_FETCH_DB_SYMBOLS_LIST),--db-symbols-list "$(KLINE_FETCH_DB_SYMBOLS_LIST)",) \
		$(if $(KLINE_FETCH_START_DATE),--start-date "$(KLINE_FETCH_START_DATE)",) \
		$(if $(KLINE_FETCH_END_DATE),--end-date "$(KLINE_FETCH_END_DATE)",) \
		$(if $(KLINE_FETCH_INTERVAL),--interval $(KLINE_FETCH_INTERVAL),) \
		$(if $(KLINE_FETCH_API_STATUS_FILTER),--api-status-filter $(KLINE_FETCH_API_STATUS_FILTER),) \
		$(if $(KLINE_FETCH_MAX_WORKERS),--max-workers $(KLINE_FETCH_MAX_WORKERS),) \
		$(if $(KLINE_FETCH_WORKER_DELAY),--worker-delay $(KLINE_FETCH_WORKER_DELAY),)

# --- Legacy/Single Kline Fetch ---
SYMBOL = $(DEFAULT_SYMBOL)
START_DATE = $(DEFAULT_START_DATE)
INTERVAL = $(DEFAULT_KLINE_INTERVAL)
fetch-single-kline: up
	@echo "Fetching klines for single symbol: $(SYMBOL)"
	$(PYTHON) -m data_ingestion.kline_ingestor --config $(CONFIG_FILE_PATH) \
		--symbol $(SYMBOL) \
		$(if $(BASE_ASSET),--base_asset $(BASE_ASSET),) \
		$(if $(QUOTE_ASSET),--quote_asset $(QUOTE_ASSET),) \
		--interval "$(INTERVAL)" \
		--start_date "$(START_DATE)" \
		$(if $(END_DATE),--end_date "$(END_DATE)",)

# --- Docker Targets ---
up:
	@echo "Starting TimescaleDB container..."
	$(DOCKER_COMPOSE) up -d
	@echo "Waiting for database to be ready..."
	@timeout_seconds=60; \
	elapsed_seconds=0; \
	until docker exec $(CONTAINER_DB_NAME) pg_isready -U $(DB_USER) -d $(DB_NAME) -q || [ $$elapsed_seconds -ge $$timeout_seconds ]; do \
		echo -n "."; sleep 1; elapsed_seconds=$$((elapsed_seconds+1)); \
	done; \
	if ! docker exec $(CONTAINER_DB_NAME) pg_isready -U $(DB_USER) -d $(DB_NAME) -q; then \
		echo " Timeout waiting for database to be ready!"; \
		exit 1; \
	fi; \
	echo " Database is ready."

init-db:
	@echo "Applying database schema from $(SCHEMA_FILE_PATH) to container $(CONTAINER_DB_NAME)..."
	@if [ ! -f ./$(SCHEMA_FILE_PATH) ]; then \
		echo "ERROR: $(SCHEMA_FILE_PATH) not found."; \
		exit 1; \
	fi
	docker exec -i $(CONTAINER_DB_NAME) psql -v ON_ERROR_STOP=1 -U $(DB_USER) -d $(DB_NAME) < ./$(SCHEMA_FILE_PATH)
	@echo "Database schema applied successfully."

down:
	@echo "Stopping and removing TimescaleDB container..."
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f timescaledb

restart:
	$(DOCKER_COMPOSE) restart timescaledb

status:
	$(DOCKER_COMPOSE) ps

db-shell: up
	@echo "Connecting to psql shell in $(CONTAINER_DB_NAME)..."
	docker exec -it $(CONTAINER_DB_NAME) psql -U $(DB_USER) -d $(DB_NAME)

# --- Other Data & Utility Targets ---
SNAPSHOT_SYMBOL = $(DEFAULT_SYMBOL)
SNAPSHOT_INTERVAL_SECONDS = 60
SNAPSHOT_LIMIT = 100
collect-orderbook-snapshots: up
	@echo "Starting periodic order book snapshot collection for $(SNAPSHOT_SYMBOL)..."
	$(PYTHON) -m data_ingestion.orderbook_ingestor --config $(CONFIG_FILE_PATH) \
		--symbol $(SNAPSHOT_SYMBOL) \
		--interval $(SNAPSHOT_INTERVAL_SECONDS) \
		--limit $(SNAPSHOT_LIMIT)

CONTINUOUS_AGGREGATES = klines_5min klines_10min klines_15min klines_30min klines_1hour klines_6hour klines_1day
refresh-aggregates: up
	@echo "Manually refreshing all continuous aggregates in $(DB_NAME)..."
	@for agg_name in $(CONTINUOUS_AGGREGATES); do \
		echo "Refreshing $$agg_name..."; \
		docker exec $(CONTAINER_DB_NAME) psql -U $(DB_USER) -d $(DB_NAME) -c "CALL refresh_continuous_aggregate('$$agg_name', NULL, NULL);"; \
	done
	@echo "All continuous aggregates refresh process initiated."

REFRESH_OVERVIEW_MVS = symbol_1m_kline_counts daily_1m_klines_ingested_stats daily_order_book_snapshots_ingested_stats
refresh-custom-mvs: up
	@echo "Refreshing custom materialized views for overview page..."
	@for mv_name in $(REFRESH_OVERVIEW_MVS); do \
		echo "Refreshing $$mv_name..."; \
		if [ "$$mv_name" = "symbol_1m_kline_counts" ]; then \
		    docker exec $(CONTAINER_DB_NAME) psql -U $(DB_USER) -d $(DB_NAME) -c "REFRESH MATERIALIZED VIEW CONCURRENTLY $$mv_name;"; \
		else \
		    docker exec $(CONTAINER_DB_NAME) psql -U $(DB_USER) -d $(DB_NAME) -c "REFRESH MATERIALIZED VIEW $$mv_name;"; \
		fi; \
	done
	@echo "Custom materialized views refresh initiated."

refresh-all-data-views: refresh-aggregates refresh-custom-mvs


# --- Web UI Target ---
webui:
	@echo "Starting Flask Web UI on http://0.0.0.0:5001 (or as configured in webui.py)..."
	PYTHONPATH=. $(PYTHON) web_ui/webui.py

# --- Cleanup Targets ---
clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete
	find . -name '*.egg-info' -type d -exec rm -rf {} +

clean-db-data:
	@echo "WARNING: This will stop the container and remove all persisted database data!"
	@read -p "Are you sure? (y/N) " choice; \
	if [ "$$choice" = "y" ] || [ "$$choice" = "Y" ]; then \
		echo "Stopping container and removing associated volumes..."; \
		$(DOCKER_COMPOSE) down -v; \
		echo "Container stopped and database data volumes removed."; \
	else \
		echo "Operation cancelled."; \
	fi