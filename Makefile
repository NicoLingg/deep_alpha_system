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
DEFAULT_START_DATE = 2017-01-01


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

# --- Vars for Cleanup ---
DELETE_FEATURE_SET_VERSION = # REQUIRED: The version part of the feature table to delete (e.g., v1_basic)

# --- Vars for Feature Engineering (Updated) ---
FEATURE_SET_VERSION = # REQUIRED: e.g., v1_basic, v2_talib_exp (used for DB table name)
FEATURE_SET_FILE = # Optional: Name or path of the YAML feature set definition file (e.g., 'default_v1' for feature_sets/default_v1.yaml, or a full path like 'feature_sets/custom.yaml'). Defaults to 'feature_sets/default.yaml' if not provided.
FEATURE_SYMBOLS =
FEATURE_INTERVALS = # Optional: Comma-separated list. If not provided, uses 'default_base_intervals_to_process' from the loaded YAML feature set.
FEATURE_START_DATE = # Optional YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
FEATURE_END_DATE = # Optional
FEATURE_RECALCULATE_ALL = # Set to "true" or "yes" to trigger --recalculate-all
FEATURE_LOOKBACK_INITIAL = # Optional: default in script is 365
FEATURE_KLINE_BUFFER_BACKWARD = # Optional: default in script (e.g., 250). For --kline-buffer-backward-periods
FEATURE_MAX_FUTURE_HORIZON_BUFFER = # Optional: default in script (e.g., 10). For --max-future-horizon-buffer-periods


.PHONY: all help build up down logs restart status db-shell init-db setup-db \
        update-symbol-definitions fetch-klines-from-db fetch-single-kline calculate-features \
        collect-orderbook-snapshots refresh-aggregates webui clean clean-db-data delete-feature-table list-feature-sets \
        refresh-custom-mvs refresh-all-data-views

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
	@echo "  calculate-features          Stage 3: Calculate features from kline data using a YAML feature set definition."
	@echo "                              Vars: FEATURE_SET_VERSION (required for DB table name),"
	@echo "                                    FEATURE_SET_FILE (optional YAML, e.g., 'my_set' for feature_sets/my_set.yaml, defaults to feature_sets/default.yaml),"
	@echo "                                    FEATURE_SYMBOLS, FEATURE_INTERVALS (overrides YAML defaults),"
	@echo "                                    FEATURE_START_DATE, FEATURE_END_DATE, FEATURE_RECALCULATE_ALL,"
	@echo "                                    FEATURE_LOOKBACK_INITIAL, FEATURE_KLINE_BUFFER_BACKWARD, FEATURE_MAX_FUTURE_HORIZON_BUFFER"
	@echo ""
	@echo "Other Data Targets:"
	@echo "  fetch-single-kline          (Legacy) Fetch klines for a single specified symbol."
	@echo "                              Vars: SYMBOL, START_DATE, END_DATE, INTERVAL, BASE_ASSET, QUOTE_ASSET"
	@echo "  collect-orderbook-snapshots Start collecting order book snapshots."
	@echo "  refresh-aggregates          Manually refresh TimescaleDB continuous aggregates."
	@echo "  refresh-custom-mvs          Manually refresh custom materialized views for overview."
	@echo "  refresh-all-data-views      Refresh both CAGGs and custom MVs."
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
	@echo "  delete-feature-table        Deletes a specified kline_features_* table."
	@echo "                              Vars: DELETE_FEATURE_SET_VERSION (required)"
	@echo "  list-feature-sets           Lists all kline_features_* tables in the database."
	@echo ""
	@echo "Example Stage 1: make update-symbol-definitions SYM_STATUSES=\"TRADING,BREAK\" SYM_PERMISSIONS=\"SPOT\""
	@echo "Example Stage 2: make fetch-klines-from-db KLINE_FETCH_DB_QUOTE_ASSET=USDT KLINE_FETCH_START_DATE=\"2022-01-01\""
	@echo "Example Stage 3 (using default YAML feature set): make calculate-features FEATURE_SET_VERSION=v1_default_yaml FEATURE_SYMBOLS=BTCUSDT"
	@echo "Example Stage 3 (using specific YAML): make calculate-features FEATURE_SET_VERSION=v2_custom FEATURE_SET_FILE=my_custom_features FEATURE_SYMBOLS=ETHUSDT"

# --- Core Workflow Targets ---
setup-db: up init-db

update-symbol-definitions: up
	@echo "Starting Stage 1: Updating symbol definitions in local DB from Binance API..."
	$(PYTHON) -m data_ingestion.manage_symbols --config $(CONFIG_FILE_PATH) \
		$(if $(SYM_STATUSES),--include-statuses "$(SYM_STATUSES)",) \
		$(if $(SYM_PERMISSIONS),--include-permissions "$(SYM_PERMISSIONS)",) \
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

# --- Feature Calculation Target (Updated) ---
calculate-features: up
	@if [ -z "$(FEATURE_SET_VERSION)" ]; then \
		echo "ERROR: FEATURE_SET_VERSION is required. Example: make calculate-features FEATURE_SET_VERSION=v1_initial"; \
		exit 1; \
	fi
	@echo "Starting feature calculation for set version (table suffix): $(FEATURE_SET_VERSION)..."
	@if [ -n "$(FEATURE_SET_FILE)" ]; then \
		echo "Using feature set definition file: $(FEATURE_SET_FILE).yaml (or path if absolute)"; \
	else \
		echo "Using default feature set definition (feature_sets/default.yaml)"; \
	fi
	PYTHONPATH=. $(PYTHON) -m feature_engineering.generator \
		--app-config $(CONFIG_FILE_PATH) \
		--feature-set-version "$(FEATURE_SET_VERSION)" \
		$(if $(FEATURE_SET_FILE),--feature-set-file "$(FEATURE_SET_FILE)",) \
		$(if $(FEATURE_SYMBOLS),--symbols "$(FEATURE_SYMBOLS)",) \
		$(if $(FEATURE_INTERVALS),--intervals "$(FEATURE_INTERVALS)",) \
		$(if $(FEATURE_START_DATE),--start-date "$(FEATURE_START_DATE)",) \
		$(if $(FEATURE_END_DATE),--end-date "$(FEATURE_END_DATE)",) \
		$(if $(filter $(FEATURE_RECALCULATE_ALL),true yes True Yes YES),--recalculate-all,) \
		$(if $(FEATURE_LOOKBACK_INITIAL),--lookback-days-initial $(FEATURE_LOOKBACK_INITIAL),) \
		$(if $(FEATURE_KLINE_BUFFER_BACKWARD),--kline-buffer-backward-periods $(FEATURE_KLINE_BUFFER_BACKWARD),) \
		$(if $(FEATURE_MAX_FUTURE_HORIZON_BUFFER),--max-future-horizon-buffer-periods $(FEATURE_MAX_FUTURE_HORIZON_BUFFER),)


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

delete-feature-table: up
	@if [ -z "$(DELETE_FEATURE_SET_VERSION)" ]; then \
		echo "ERROR: DELETE_FEATURE_SET_VERSION is required. Example: make delete-feature-table DELETE_FEATURE_SET_VERSION=v1_old_experiment"; \
		exit 1; \
	fi
	$(eval VERSION_SANITIZED := $(shell echo "$(DELETE_FEATURE_SET_VERSION)" | tr '[:upper:]' '[:lower:]' | sed 's/-/_/g' | sed 's/\./_/g'))
	$(eval TABLE_TO_DELETE := kline_features_$(VERSION_SANITIZED))
	@echo "Attempting to delete feature table: $(TABLE_TO_DELETE) from database $(DB_NAME)...";
	@bash -c '\
		table_to_delete_arg="$${1}"; \
		container_db_name_arg="$${2}"; \
		db_user_arg="$${3}"; \
		db_name_arg="$${4}"; \
		read -r -p "Are you SURE you want to PERMANENTLY DELETE table '\''$$table_to_delete_arg'\''? (y/N) " choice; \
		if [[ "$$choice" == [yY] || "$$choice" == [yY][eE][sS] ]]; then \
			echo "Deleting table $$table_to_delete_arg..."; \
			docker exec "$$container_db_name_arg" psql -U "$$db_user_arg" -d "$$db_name_arg" -c "DROP TABLE IF EXISTS \"$$table_to_delete_arg\" CASCADE;"; \
			echo "Table $$table_to_delete_arg deleted (if it existed)."; \
		else \
			echo "Operation cancelled. Table $$table_to_delete_arg was NOT deleted."; \
		fi \
	' bash "$(TABLE_TO_DELETE)" "$(CONTAINER_DB_NAME)" "$(DB_USER)" "$(DB_NAME)"

list-feature-sets: up
	@echo "Listing all kline feature set tables from database $(DB_NAME)..."
	@echo "----------------------------------------------------------------"
	@docker exec $(CONTAINER_DB_NAME) psql -U $(DB_USER) -d $(DB_NAME) -c "\
	SELECT tablename \
	FROM pg_tables \
	WHERE schemaname = 'public' AND tablename LIKE 'kline_features_%' \
	ORDER BY tablename;"
	@echo "----------------------------------------------------------------"
	@echo "Done."

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