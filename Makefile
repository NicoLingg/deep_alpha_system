# --- Configuration ---
PYTHON = python3
DOCKER_COMPOSE = docker-compose
CONTAINER_DB_NAME = deep_alpha_timescaledb
DB_USER = deep_alpha_user
DB_NAME = deep_alpha_data_db

CONFIG_FILE_PATH = config/config.ini
SCHEMA_FILE_PATH = db/schema.sql

# Default symbol and interval for data fetching
DEFAULT_SYMBOL = BTCUSDT
DEFAULT_KLINE_INTERVAL = 1m
DEFAULT_START_DATE = 2025-03-01

# Symbols for bulk kline fetching
BULK_SYMBOLS = BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT ADAUSDT DOGEUSDT

CONTINUOUS_AGGREGATES = klines_5min klines_10min klines_15min klines_30min klines_1hour klines_6hour klines_1day

# Default for snapshot collector
DEFAULT_SNAPSHOT_SYMBOL = $(DEFAULT_SYMBOL)
DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 60
DEFAULT_SNAPSHOT_LIMIT = 100

.PHONY: all help build up down logs restart status db-shell init-db \
        fetch-klines fetch-klines-bulk \
        collect-orderbook-snapshots get-liquid-symbols \
        refresh-aggregates webui clean clean-db-data

all: up init-db # Common default target

help:
	@echo "Market Data Pipeline Makefile"
	@echo ""
	@echo "Usage: make [target] [VAR=value ...]"
	@echo ""
	@echo "Core Targets:"
	@echo "  up                          Start database container."
	@echo "  init-db                     Initialize database schema (run after 'up')."
	@echo "  fetch-klines                Fetch 1m klines for SYMBOL (default: $(DEFAULT_SYMBOL))."
	@echo "                              Vars: SYMBOL, START_DATE, END_DATE"
	@echo "  fetch-klines-bulk           Fetch 1m klines for symbols in BULK_SYMBOLS."
	@echo "                              Vars: START_DATE, END_DATE, BULK_SYMBOLS"
	@echo "  collect-orderbook-snapshots Start collecting periodic order book snapshots."
	@echo "                              Vars: SNAPSHOT_SYMBOL, SNAPSHOT_INTERVAL_SECONDS, SNAPSHOT_LIMIT"
	@echo "  get-liquid-symbols          List top liquid symbols from Binance."
	@echo "                              Vars: TOP_N, QUOTE_ASSET, MIN_VOLUME"
	@echo "  refresh-aggregates          Manually refresh continuous aggregates."
	@echo "  webui                       Start the Flask web UI."
	@echo ""
	@echo "Docker & Maintenance Targets:"
	@echo "  build                       Build/rebuild Docker services (if custom Dockerfiles involved)."
	@echo "  down                        Stop and remove database container."
	@echo "  logs                        Show logs for the database container."
	@echo "  restart                     Restart the database container."
	@echo "  status                      Show status of Docker services."
	@echo "  db-shell                    Open a psql shell to the database."
	@echo "  clean                       Remove Python bytecode and __pycache__."
	@echo "  clean-db-data               WARNING: Stops container and removes database data volume."
	@echo ""
	@echo "Example: make fetch-klines SYMBOL=ETHUSDT START_DATE=\"2023-01-01\""
	@echo "Example: make collect-orderbook-snapshots SNAPSHOT_SYMBOL=LTCUSDT SNAPSHOT_INTERVAL_SECONDS=30"

# --- Docker Targets ---
build:
	$(DOCKER_COMPOSE) build

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

# --- Database Initialization ---
init-db: up
	@echo "Applying database schema from $(SCHEMA_FILE_PATH) to container $(CONTAINER_DB_NAME)..."
	@if [ ! -f ./$(SCHEMA_FILE_PATH) ]; then \
		echo "ERROR: $(SCHEMA_FILE_PATH) not found."; \
		exit 1; \
	fi
	docker exec -i $(CONTAINER_DB_NAME) psql -v ON_ERROR_STOP=1 -U $(DB_USER) -d $(DB_NAME) < ./$(SCHEMA_FILE_PATH)
	@echo "Database schema applied successfully."

# --- Continuous Aggregate Refresh Target ---
refresh-aggregates: up
	@echo "Manually refreshing all continuous aggregates in $(DB_NAME)..."
	@for agg_name in $(CONTINUOUS_AGGREGATES); do \
		echo "Refreshing $$agg_name..."; \
		docker exec $(CONTAINER_DB_NAME) psql -U $(DB_USER) -d $(DB_NAME) -c "CALL refresh_continuous_aggregate('$$agg_name', NULL, NULL);"; \
	done
	@echo "All continuous aggregates refresh process initiated."


# --- Data Ingestion Targets ---
# Variables for single kline fetch (can be overridden)
SYMBOL = $(DEFAULT_SYMBOL)
START_DATE = $(DEFAULT_START_DATE)

fetch-klines: up
	@echo "Fetching 1-minute klines for $(SYMBOL) from $(START_DATE)..."
	$(PYTHON) -m data_ingestion.kline_ingestor --config $(CONFIG_FILE_PATH) \
		--symbol $(SYMBOL) \
		--interval "$(DEFAULT_KLINE_INTERVAL)" \
		--start_date "$(START_DATE)" \
		$(if $(END_DATE),--end_date "$(END_DATE)",)

# Variables for bulk kline fetch (START_DATE and END_DATE can be overridden)
fetch-klines-bulk: up
	@echo "Starting bulk fetch of 1-minute klines from $(START_DATE) for symbols: $(BULK_SYMBOLS)"
	@if [ -z "$(BULK_SYMBOLS)" ]; then \
		echo "ERROR: BULK_SYMBOLS variable is empty in Makefile. Please define a list of symbols."; \
		exit 1; \
	fi
	@for sym_name in $(BULK_SYMBOLS); do \
		echo "-----------------------------------------------------"; \
		echo "Fetching 1-minute klines for $$sym_name from $(START_DATE)..."; \
		echo "-----------------------------------------------------"; \
		$(PYTHON) -m data_ingestion.kline_ingestor --config $(CONFIG_FILE_PATH) \
			--symbol $$sym_name \
			--interval "$(DEFAULT_KLINE_INTERVAL)" \
			--start_date "$(START_DATE)" \
			$(if $(END_DATE),--end_date "$(END_DATE)",); \
		echo "Sleeping for 5 seconds before next symbol..."; \
		sleep 5; \
	done
	@echo "Bulk kline fetch completed."

# --- Order Book Snapshot Collector Targets ---
SNAPSHOT_SYMBOL = $(DEFAULT_SNAPSHOT_SYMBOL)
SNAPSHOT_INTERVAL_SECONDS = $(DEFAULT_SNAPSHOT_INTERVAL_SECONDS)
SNAPSHOT_LIMIT = $(DEFAULT_SNAPSHOT_LIMIT)

collect-orderbook-snapshots: up
	@echo "Starting periodic order book snapshot collection for $(SNAPSHOT_SYMBOL) every $(SNAPSHOT_INTERVAL_SECONDS)s with limit $(SNAPSHOT_LIMIT)..."
	$(PYTHON) -m data_ingestion.orderbook_ingestor --config $(CONFIG_FILE_PATH) \
		--symbol $(SNAPSHOT_SYMBOL) \
		--interval $(SNAPSHOT_INTERVAL_SECONDS) \
		--limit $(SNAPSHOT_LIMIT)

# --- Symbol Utilities ---
TOP_N = 20
QUOTE_ASSET = USDT
MIN_VOLUME = 1000000

get-liquid-symbols:
	@echo "Fetching top $(TOP_N) liquid symbols (Quote: $(QUOTE_ASSET), Min Volume: $(MIN_VOLUME))..."
	$(PYTHON) -m data_ingestion.symbol_utils --config $(CONFIG_FILE_PATH) \
		--top_n $(TOP_N) \
		--quote_asset "$(QUOTE_ASSET)" \
		--min_volume $(MIN_VOLUME)


# --- Web UI Target ---
webui:
	@echo "Starting Flask Web UI on http://0.0.0.0:5001 ..."
	# Assuming webui.py in web_ui/ can correctly find config file using relative paths
	# and has if __name__ == "__main__": app.run(...)
	# To ensure PYTHONPATH is set correctly for imports within web_ui if any:
	PYTHONPATH=. $(PYTHON) web_ui/webui.py
	# Or simply:
	# cd web_ui && $(PYTHON) webui.py

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