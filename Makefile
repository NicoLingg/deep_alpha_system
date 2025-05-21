# Makefile for Deep Alpha System

# --- Configuration ---
PYTHON = python3
DOCKER_COMPOSE = docker-compose

# Default config path - can be overridden by setting CONFIG_FILE_PATH environment variable or make var
CONFIG_FILE_PATH ?= config/config.ini
SCHEMA_FILE_PATH = db/schema.sql

# Docker DB settings (attempt to read from config.ini, with fallbacks)
# Note: This shell command runs when 'make' starts. If config.ini is not found initially, it uses fallbacks.
DB_USER := $(shell $(PYTHON) -c "import configparser, os; c=configparser.ConfigParser(); p='$(CONFIG_FILE_PATH)'; print(c.get('database','user','deep_alpha_user') if c.read(p) and c.has_section('database') and c.has_option('database','user') else 'deep_alpha_user')" 2>/dev/null || echo "deep_alpha_user")
DB_NAME := $(shell $(PYTHON) -c "import configparser, os; c=configparser.ConfigParser(); p='$(CONFIG_FILE_PATH)'; print(c.get('database','dbname','deep_alpha_data_db') if c.read(p) and c.has_section('database') and c.has_option('database','dbname') else 'deep_alpha_data_db')" 2>/dev/null || echo "deep_alpha_data_db")
CONTAINER_DB_NAME = deep_alpha_timescaledb # This should match your docker-compose.yml service name


# --- CLI Script Base Command ---
# Ensures that the scripts are run as modules from the project root directory context
# and that the local data_ingestion (etc.) package is in PYTHONPATH.
RUN_SCRIPT = PYTHONPATH=. $(PYTHON) -m data_ingestion

# --- Variables for Stage 1: Update Symbol Definitions ---
SYM_EXCHANGE ?= binance # Default exchange for symbol definition update
# SYM_QUOTE_ASSET is not directly used by manage_symbols.py for API filtering,
# but could be used if the script logic was extended.

# --- Variables for Stage 2: Fetch Klines for symbols in DB ---
KLINE_FETCH_DB_EXCHANGE_NAME ?= # Optional: filter DB symbols by exchange_name for kline fetching (e.g., binance).
KLINE_FETCH_DB_QUOTE_ASSET ?= # Optional: filter DB symbols by *standardized* quote_asset (e.g., USDT).
KLINE_FETCH_DB_INSTRUMENT_LIKE ?= # Optional: SQL LIKE for *exchange_instrument_name* from DB (e.g., %BTC%).
KLINE_FETCH_DB_SYMBOLS_LIST ?= # Optional: comma-separated list of *standard* symbols from DB (e.g., BTC-USDT,ETH-USD-PERP). Overrides other DB filters.
KLINE_FETCH_START_DATE ?= # Uses config: ingestion.default_start_date if empty.
KLINE_FETCH_END_DATE ?= # Optional: for kline end date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ). Default: latest.
KLINE_FETCH_INTERVAL ?= # Uses config: ingestion.default_kline_interval if empty.
KLINE_FETCH_API_STATUS_FILTER ?= # Uses config: symbol_management.default_kline_fetch_api_status_filter if empty.
KLINE_FETCH_MAX_WORKERS ?= # Uses config: ingestion.max_workers if empty.
KLINE_FETCH_WORKER_DELAY ?= # Uses config: ingestion.worker_symbol_delay if empty.

# --- Variables for Single Kline Fetch (primarily for testing/dev) ---
SINGLE_KLINE_EXCHANGE ?= binance
SINGLE_KLINE_SYMBOL ?= BTC-USDT 
SINGLE_KLINE_START_DATE ?= 2023-01-01T00:00:00Z
SINGLE_KLINE_END_DATE ?= 
SINGLE_KLINE_INTERVAL ?= 1m
# Optional: --base-asset, --quote-asset, --instrument-type if --symbol is not self-descriptive or needs override
SINGLE_KLINE_BASE_ASSET ?=
SINGLE_KLINE_QUOTE_ASSET ?=
SINGLE_KLINE_INSTRUMENT_TYPE ?=

# --- Variables for Orderbook Snapshot Collection ---
SNAPSHOT_EXCHANGE ?= binance
SNAPSHOT_SYMBOL ?= BTC-USDT
SNAPSHOT_INTERVAL_SECONDS ?= 60
SNAPSHOT_LIMIT ?= 100

# --- Variables for Feature Engineering ---
# FEATURE_SET_VERSION is REQUIRED for naming the output table: kline_features_<version>
FEATURE_SET_VERSION ?=
FEATURE_SET_FILE ?= # Optional: Name of YAML in feature_sets/ (e.g., 'v1_basic' for v1_basic.yaml) or full path. Defaults to 'default.yaml'.
FEATURE_SYMBOLS ?= # Optional: Comma-separated STANDARD symbols (e.g., BTC-USDT,ETH-USD-PERP). Overrides YAML.
FEATURE_INTERVALS ?= # Optional: Comma-separated intervals (e.g., 1m,5m,1h). Overrides YAML.
FEATURE_START_DATE ?= # Optional: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ.
FEATURE_END_DATE ?= # Optional.
FEATURE_RECALCULATE_ALL ?= # Set to "true" or "yes" to trigger --recalculate-all.
FEATURE_MAX_WORKERS ?= # Optional: Max parallel tasks for feature generation.

# --- Variables for Cleanup ---
DELETE_FEATURE_SET_VERSION ?= # REQUIRED: The version part of the feature table to delete (e.g., v1_basic)

# --- Variables for Test Liquid Klines Script ---
TEST_LIQUID_TOP_N ?= 5
TEST_LIQUID_QUOTE_ASSET ?= USDT
TEST_LIQUID_MIN_VOLUME ?= 1000000
TEST_LIQUID_KLINE_INTERVAL ?= 1m
TEST_LIQUID_KLINE_START_DATE ?= # Defaults to 7 days ago in script
TEST_LIQUID_KLINE_END_DATE ?=   # Defaults to now in script
TEST_LIQUID_KLINE_LIMIT ?= 10


.PHONY: all help build up down logs restart status db-shell init-db setup-db \
        update-symbol-definitions fetch-klines-from-db fetch-single-kline calculate-features \
        collect-orderbook-snapshots refresh-caggs refresh-custom-mvs refresh-all-data-views \
        webui clean clean-db-data delete-feature-table list-feature-tables list-symbols-from-db test-liquid-klines

all: help

help:
	@echo "Deep Alpha System Makefile"
	@echo ""
	@echo "Usage: make [TARGET] [VAR=value ...]"
	@echo ""
	@echo "Core Workflow Targets (Recommended Order):"
	@echo "  setup-db                    Shortcut: Combines 'up' and 'init-db'."
	@echo "  update-symbol-definitions   Stage 1: Fetch all symbol definitions from an exchange and store/update in local DB."
	@echo "                              Vars: SYM_EXCHANGE (default: binance)"
	@echo "  fetch-klines-from-db        Stage 2: Fetch klines for symbols ALREADY IN LOCAL DB, based on filters."
	@echo "                              Vars: KLINE_FETCH_DB_EXCHANGE_NAME, KLINE_FETCH_DB_QUOTE_ASSET, KLINE_FETCH_DB_INSTRUMENT_LIKE,"
	@echo "                                    KLINE_FETCH_DB_SYMBOLS_LIST (overrides other DB filters if set),"
	@echo "                                    KLINE_FETCH_START_DATE, KLINE_FETCH_END_DATE, KLINE_FETCH_INTERVAL,"
	@echo "                                    KLINE_FETCH_API_STATUS_FILTER, KLINE_FETCH_MAX_WORKERS, KLINE_FETCH_WORKER_DELAY"
	@echo "  calculate-features          Stage 3: Calculate features from kline data using a YAML feature set definition."
	@echo "                              Required Var: FEATURE_SET_VERSION (e.g., v1_basic for table kline_features_v1_basic)"
	@echo "                              Optional Vars: FEATURE_SET_FILE, FEATURE_SYMBOLS, FEATURE_INTERVALS, FEATURE_START_DATE, FEATURE_END_DATE, FEATURE_RECALCULATE_ALL, FEATURE_MAX_WORKERS"
	@echo ""
	@echo "Other Data Collection & Management Targets:"
	@echo "  fetch-single-kline          Fetch klines for a single specified symbol (dev/test)."
	@echo "                              Vars: SINGLE_KLINE_EXCHANGE, SINGLE_KLINE_SYMBOL, SINGLE_KLINE_START_DATE, SINGLE_KLINE_END_DATE, SINGLE_KLINE_INTERVAL, SINGLE_KLINE_BASE_ASSET, SINGLE_KLINE_QUOTE_ASSET, SINGLE_KLINE_INSTRUMENT_TYPE"
	@echo "  collect-orderbook-snapshots Start collecting order book snapshots for a symbol."
	@echo "                              Vars: SNAPSHOT_EXCHANGE, SNAPSHOT_SYMBOL, SNAPSHOT_INTERVAL_SECONDS, SNAPSHOT_LIMIT"
	@echo "  refresh-caggs               Manually refresh TimescaleDB continuous aggregates."
	@echo "  refresh-custom-mvs          Manually refresh custom (non-CAGG) materialized views for overview."
	@echo "  refresh-all-data-views      Refresh both CAGGs and custom MVs."
	@echo "  list-symbols-from-db        Utility to list symbols from the DB based on filters (uses populate_db.py dry-run like logic)."
	@echo ""
	@echo "Service & DB Container Targets:"
	@echo "  up                          Start database Docker container."
	@echo "  init-db                     Initialize database schema (run after 'up' if DB is new/empty)."
	@echo "  down                        Stop and remove database Docker container."
	@echo "  logs                        Show logs for the database container."
	@echo "  restart                     Restart the database container."
	@echo "  status                      Show status of Docker services."
	@echo "  db-shell                    Open a psql shell to the database."
	@echo ""
	@echo "Web UI & Testing:"
	@echo "  webui                       Start the Flask web UI (experimental)."
	@echo "  test-liquid-klines          Run test_binance_liquid_klines.py script."
	@echo "                              Vars: TEST_LIQUID_TOP_N, TEST_LIQUID_QUOTE_ASSET, TEST_LIQUID_MIN_VOLUME, TEST_LIQUID_KLINE_INTERVAL, TEST_LIQUID_KLINE_START_DATE, TEST_LIQUID_KLINE_END_DATE, TEST_LIQUID_KLINE_LIMIT"
	@echo ""
	@echo "Cleanup Targets:"
	@echo "  clean                       Remove Python bytecode (__pycache__) and .egg-info."
	@echo "  clean-db-data               WARNING: Stops container and removes database data volume (prompts for confirmation)."
	@echo "  delete-feature-table        Deletes a specified kline_features_* table (prompts for confirmation)."
	@echo "                              Required Var: DELETE_FEATURE_SET_VERSION (e.g., v1_old)"
	@echo "  list-feature-tables         Lists all kline_features_* tables in the database."
	@echo ""
	@echo "Example Stage 1: make update-symbol-definitions SYM_EXCHANGE=binance"
	@echo "Example Stage 2: make fetch-klines-from-db KLINE_FETCH_DB_EXCHANGE_NAME=binance KLINE_FETCH_DB_QUOTE_ASSET=USDT KLINE_FETCH_START_DATE=\"2024-01-01T00:00:00Z\""
	@echo "Example Stage 3: make calculate-features FEATURE_SET_VERSION=v1_myfeatures FEATURE_SYMBOLS=BTC-USDT,ETH-USDT FEATURE_SET_FILE=my_set"

# --- Docker Targets ---
up:
	@echo "Starting TimescaleDB container (service name in docker-compose: timescaledb)..."
	$(DOCKER_COMPOSE) up -d timescaledb
	@echo "Waiting for database to be ready (user: $(DB_USER), db: $(DB_NAME) on container $(CONTAINER_DB_NAME))..."
	@timeout_seconds=60; \
	elapsed_seconds=0; \
	until docker exec $(CONTAINER_DB_NAME) pg_isready -U "$(DB_USER)" -d "$(DB_NAME)" -q || [ $$elapsed_seconds -ge $$timeout_seconds ]; do \
		echo -n "."; sleep 1; elapsed_seconds=$$((elapsed_seconds+1)); \
	done; \
	if ! docker exec $(CONTAINER_DB_NAME) pg_isready -U "$(DB_USER)" -d "$(DB_NAME)" -q; then \
		echo ;\
		echo "ERROR: Timeout waiting for database to be ready!"; \
		echo "Ensure '$(CONTAINER_DB_NAME)' is the correct container name and '$(DB_USER)'/'$(DB_NAME)' are correct."; \
		echo "Check docker-compose logs: make logs"; \
		exit 1; \
	fi; \
	echo " Database is ready."

init-db: up
	@echo "Applying database schema from $(SCHEMA_FILE_PATH) to container $(CONTAINER_DB_NAME)..."
	@if [ ! -f "$(SCHEMA_FILE_PATH)" ]; then \
		echo "ERROR: Schema file $(SCHEMA_FILE_PATH) not found in $(shell pwd)."; \
		exit 1; \
	fi
	@echo "Using DB User: $(DB_USER), DB Name: $(DB_NAME)"
	docker exec -i $(CONTAINER_DB_NAME) psql -v ON_ERROR_STOP=1 -U "$(DB_USER)" -d "$(DB_NAME)" < "$(SCHEMA_FILE_PATH)"
	@echo "Database schema applied successfully."

setup-db: up init-db

down:
	@echo "Stopping and removing TimescaleDB container and related volumes..."
	$(DOCKER_COMPOSE) down -v # -v removes volumes defined in docker-compose.yml

logs:
	$(DOCKER_COMPOSE) logs -f timescaledb

restart:
	$(DOCKER_COMPOSE) restart timescaledb

status:
	$(DOCKER_COMPOSE) ps

db-shell: up
	@echo "Connecting to psql shell in $(CONTAINER_DB_NAME) as user $(DB_USER) to db $(DB_NAME)..."
	docker exec -it $(CONTAINER_DB_NAME) psql -U "$(DB_USER)" -d "$(DB_NAME)"

# --- Core Workflow Targets ---
update-symbol-definitions: up
	@echo "Starting Stage 1: Updating symbol definitions in local DB from $(SYM_EXCHANGE) API..."
	$(RUN_SCRIPT).manage_symbols --config "$(CONFIG_FILE_PATH)" --exchange "$(SYM_EXCHANGE)"

fetch-klines-from-db: up
	@echo "Starting Stage 2: Fetching klines for symbols defined in local DB..."
	$(RUN_SCRIPT).populate_db --config "$(CONFIG_FILE_PATH)" \
		$(if $(KLINE_FETCH_DB_EXCHANGE_NAME),--db-exchange-name "$(KLINE_FETCH_DB_EXCHANGE_NAME)",) \
		$(if $(KLINE_FETCH_DB_QUOTE_ASSET),--db-quote-asset "$(KLINE_FETCH_DB_QUOTE_ASSET)",) \
		$(if $(KLINE_FETCH_DB_INSTRUMENT_LIKE),--db-instrument-like "$(KLINE_FETCH_DB_INSTRUMENT_LIKE)",) \
		$(if $(KLINE_FETCH_DB_SYMBOLS_LIST),--db-symbols-list "$(KLINE_FETCH_DB_SYMBOLS_LIST)",) \
		$(if $(KLINE_FETCH_START_DATE),--start-date "$(KLINE_FETCH_START_DATE)",) \
		$(if $(KLINE_FETCH_END_DATE),--end-date "$(KLINE_FETCH_END_DATE)",) \
		$(if $(KLINE_FETCH_INTERVAL),--interval "$(KLINE_FETCH_INTERVAL)",) \
		$(if $(KLINE_FETCH_API_STATUS_FILTER),--api-status-filter "$(KLINE_FETCH_API_STATUS_FILTER)",) \
		$(if $(KLINE_FETCH_MAX_WORKERS),--max-workers $(KLINE_FETCH_MAX_WORKERS),) \
		$(if $(KLINE_FETCH_WORKER_DELAY),--worker-delay $(KLINE_FETCH_WORKER_DELAY),)

calculate-features: up
	@if [ -z "$(FEATURE_SET_VERSION)" ]; then \
		echo "ERROR: FEATURE_SET_VERSION is required. Example: make calculate-features FEATURE_SET_VERSION=v1_initial"; \
		exit 1; \
	fi
	@echo "Starting feature calculation for set version (table suffix): $(FEATURE_SET_VERSION)..."
	@if [ -n "$(FEATURE_SET_FILE)" ]; then \
		echo "Using feature set definition file: $(FEATURE_SET_FILE) (expected in feature_sets/ or as full path)"; \
	else \
		echo "Using default feature set definition (e.g., feature_sets/default.yaml or as per script default)"; \
	fi
	PYTHONPATH=. $(PYTHON) -m feature_engineering.generator \
		--app-config "$(CONFIG_FILE_PATH)" \
		--feature-set-version "$(FEATURE_SET_VERSION)" \
		$(if $(FEATURE_SET_FILE),--feature-set-file "$(FEATURE_SET_FILE)",) \
		$(if $(FEATURE_SYMBOLS),--symbols "$(FEATURE_SYMBOLS)",) \
		$(if $(FEATURE_INTERVALS),--intervals "$(FEATURE_INTERVALS)",) \
		$(if $(FEATURE_START_DATE),--start-date "$(FEATURE_START_DATE)",) \
		$(if $(FEATURE_END_DATE),--end-date "$(FEATURE_END_DATE)",) \
		$(if $(filter $(FEATURE_RECALCULATE_ALL),true yes True Yes YES),--recalculate-all,) \
		$(if $(FEATURE_MAX_WORKERS),--max-workers $(FEATURE_MAX_WORKERS),)

# --- Other Data Targets ---
fetch-single-kline: up
	@echo "Fetching klines for single symbol: $(SINGLE_KLINE_SYMBOL) on $(SINGLE_KLINE_EXCHANGE)"
	$(RUN_SCRIPT).kline_ingestor --config "$(CONFIG_FILE_PATH)" \
		--exchange "$(SINGLE_KLINE_EXCHANGE)" \
		--symbol "$(SINGLE_KLINE_SYMBOL)" \
		--interval "$(SINGLE_KLINE_INTERVAL)" \
		--start-date "$(SINGLE_KLINE_START_DATE)" \
		$(if $(SINGLE_KLINE_END_DATE),--end-date "$(SINGLE_KLINE_END_DATE)",) \
		$(if $(SINGLE_KLINE_BASE_ASSET),--base-asset "$(SINGLE_KLINE_BASE_ASSET)",) \
		$(if $(SINGLE_KLINE_QUOTE_ASSET),--quote-asset "$(SINGLE_KLINE_QUOTE_ASSET)",) \
		$(if $(SINGLE_KLINE_INSTRUMENT_TYPE),--instrument-type "$(SINGLE_KLINE_INSTRUMENT_TYPE)",)

collect-orderbook-snapshots: up
	@echo "Starting periodic order book snapshot collection for $(SNAPSHOT_SYMBOL) on $(SNAPSHOT_EXCHANGE)..."
	$(RUN_SCRIPT).orderbook_ingestor --config "$(CONFIG_FILE_PATH)" \
		--exchange "$(SNAPSHOT_EXCHANGE)" \
		--symbol "$(SNAPSHOT_SYMBOL)" \
		--interval $(SNAPSHOT_INTERVAL_SECONDS) \
		--limit $(SNAPSHOT_LIMIT)

# Define CAGGs and MVs as found in schema.sql (adjust if schema changes)
CONTINUOUS_AGGREGATES = klines_5min klines_10min klines_15min klines_30min klines_1hour klines_4hour klines_6hour klines_12hour klines_1day
REFRESH_CUSTOM_MVS = symbol_1m_kline_stats daily_1m_klines_ingestion_stats daily_orderbook_ingestion_stats

refresh-caggs: up
	@echo "Manually refreshing all continuous aggregates in $(DB_NAME)..."
	@for agg_name in $(CONTINUOUS_AGGREGATES); do \
		echo "Refreshing CAGG: $$agg_name..."; \
		docker exec $(CONTAINER_DB_NAME) psql -U "$(DB_USER)" -d "$(DB_NAME)" -c "CALL refresh_continuous_aggregate('$$agg_name', NULL, NULL);" || echo "WARN: Failed to refresh CAGG $$agg_name (it might not exist or policy not running)"; \
	done
	@echo "All continuous aggregates refresh process initiated."

refresh-custom-mvs: up
	@echo "Refreshing custom materialized views for overview page..."
	@for mv_name in $(REFRESH_CUSTOM_MVS); do \
		echo "Refreshing MV: $$mv_name..."; \
		if [ "$$mv_name" = "symbol_1m_kline_stats" ] || [ "$$mv_name" = "daily_1m_klines_ingestion_stats" ] || [ "$$mv_name" = "daily_orderbook_ingestion_stats" ] ; then \
		    docker exec $(CONTAINER_DB_NAME) psql -U "$(DB_USER)" -d "$(DB_NAME)" -c "REFRESH MATERIALIZED VIEW CONCURRENTLY $$mv_name;" || \
		    (echo "WARN: CONCURRENTLY refresh failed for $$mv_name, trying without..." && \
		     docker exec $(CONTAINER_DB_NAME) psql -U "$(DB_USER)" -d "$(DB_NAME)" -c "REFRESH MATERIALIZED VIEW $$mv_name;"); \
		else \
		    docker exec $(CONTAINER_DB_NAME) psql -U "$(DB_USER)" -d "$(DB_NAME)" -c "REFRESH MATERIALIZED VIEW $$mv_name;"; \
		fi; \
	done
	@echo "Custom materialized views refresh initiated."

refresh-all-data-views: refresh-caggs refresh-custom-mvs

list-symbols-from-db: up
	@echo "Listing symbols from DB (using populate_db.py symbol selection logic)..."
	$(RUN_SCRIPT).populate_db --config "$(CONFIG_FILE_PATH)" --max-workers 1 \
		$(if $(KLINE_FETCH_DB_EXCHANGE_NAME),--db-exchange-name "$(KLINE_FETCH_DB_EXCHANGE_NAME)",) \
		$(if $(KLINE_FETCH_DB_QUOTE_ASSET),--db-quote-asset "$(KLINE_FETCH_DB_QUOTE_ASSET)",) \
		$(if $(KLINE_FETCH_DB_INSTRUMENT_LIKE),--db-instrument-like "$(KLINE_FETCH_DB_INSTRUMENT_LIKE)",) \
		$(if $(KLINE_FETCH_DB_SYMBOLS_LIST),--db-symbols-list "$(KLINE_FETCH_DB_SYMBOLS_LIST)",) \
		--start-date "1970-01-01T00:00:00Z" --end-date "1970-01-01T00:00:01Z" # Dummy dates for dry run effect

# --- Web UI & Testing Targets ---
webui:
	@echo "Starting Flask Web UI (ensure web_ui/webui.py exists and is configured)..."
	@echo "Default: http://0.0.0.0:5001 (or as configured in webui.py)"
	PYTHONPATH=. $(PYTHON) web_ui/webui.py

test-liquid-klines: up
	@echo "Running test script for Binance top liquid symbol klines..."
	$(RUN_SCRIPT).test_binance_liquid_klines --config "$(CONFIG_FILE_PATH)" \
		$(if $(TEST_LIQUID_TOP_N),--top-n-symbols $(TEST_LIQUID_TOP_N),) \
		$(if $(TEST_LIQUID_QUOTE_ASSET),--quote-asset "$(TEST_LIQUID_QUOTE_ASSET)",) \
		$(if $(TEST_LIQUID_MIN_VOLUME),--min-volume $(TEST_LIQUID_MIN_VOLUME),) \
		$(if $(TEST_LIQUID_KLINE_INTERVAL),--interval "$(TEST_LIQUID_KLINE_INTERVAL)",) \
		$(if $(TEST_LIQUID_KLINE_START_DATE),--start-date "$(TEST_LIQUID_KLINE_START_DATE)",) \
		$(if $(TEST_LIQUID_KLINE_END_DATE),--end-date "$(TEST_LIQUID_KLINE_END_DATE)",) \
		$(if $(TEST_LIQUID_KLINE_LIMIT),--kline-limit $(TEST_LIQUID_KLINE_LIMIT),)

# --- Cleanup Targets ---
clean:
	find . -type f -name '*.py[co]' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type d -name '.pytest_cache' -exec rm -rf {} +
	find . -type d -name '*.egg-info' -exec rm -rf {} +
	@echo "Cleaned Python bytecode, __pycache__, .pytest_cache, and .egg-info directories."

clean-db-data:
	@echo "WARNING: This will stop the Docker container and PERMANENTLY REMOVE all persisted database data!"
	@read -p "Are you absolutely sure you want to proceed? (Type 'yes' to confirm): " choice; \
	if [ "$$choice" = "yes" ]; then \
		echo "Stopping container and removing associated volumes..."; \
		$(DOCKER_COMPOSE) down -v; \
		echo "Container stopped and database data volumes removed."; \
	else \
		echo "Operation cancelled. Database data was NOT removed."; \
	fi

delete-feature-table: up
	@if [ -z "$(DELETE_FEATURE_SET_VERSION)" ]; then \
		echo "ERROR: DELETE_FEATURE_SET_VERSION is required. Example: make delete-feature-table DELETE_FEATURE_SET_VERSION=v1_old_experiment"; \
		exit 1; \
	fi
	$(eval VERSION_SANITIZED := $(shell echo "$(DELETE_FEATURE_SET_VERSION)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g'))
	$(eval TABLE_TO_DELETE := kline_features_$(VERSION_SANITIZED))
	@echo "Attempting to delete feature table: $(TABLE_TO_DELETE) from database $(DB_NAME)...";
	@bash -c '\
		table_to_delete_arg="$${1}"; \
		container_db_name_arg="$${2}"; \
		db_user_arg="$${3}"; \
		db_name_arg="$${4}"; \
		read -r -p "Are you SURE you want to PERMANENTLY DELETE table '\''$$table_to_delete_arg'\'' from database '\''$$db_name_arg'\''? (Type '\''yes'\'' to confirm): " choice; \
		if [[ "$$choice" == "yes" ]]; then \
			echo "Deleting table $$table_to_delete_arg..."; \
			docker exec "$$container_db_name_arg" psql -U "$$db_user_arg" -d "$$db_name_arg" -c "DROP TABLE IF EXISTS \"public\".\"$$table_to_delete_arg\" CASCADE;"; \
			echo "Table $$table_to_delete_arg deleted (if it existed)."; \
		else \
			echo "Operation cancelled. Table $$table_to_delete_arg was NOT deleted."; \
		fi \
	' bash "$(TABLE_TO_DELETE)" "$(CONTAINER_DB_NAME)" "$(DB_USER)" "$(DB_NAME)"

list-feature-tables: up
	@echo "Listing all kline feature set tables (kline_features_*) from database $(DB_NAME)..."
	@echo "----------------------------------------------------------------"
	@docker exec $(CONTAINER_DB_NAME) psql -U "$(DB_USER)" -d "$(DB_NAME)" -c "\
	SELECT table_schema, tablename \
	FROM information_schema.tables \
	WHERE table_schema = 'public' AND tablename LIKE 'kline_features_%' \
	ORDER BY tablename;"
	@echo "----------------------------------------------------------------"
	@echo "Done."