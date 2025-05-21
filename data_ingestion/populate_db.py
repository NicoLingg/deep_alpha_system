# data_ingestion/populate_db.py
import time
import asyncio
import argparse
import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd
from tqdm import tqdm  # Keep tqdm for visible progress bar
from typing import Optional, List, Dict, Any, Tuple
import logging

from .utils import (
    load_config,
    get_db_connection,
    get_exchange_adapter,
    DEFAULT_CONFIG_PATH,
    setup_logging,
)
from .kline_ingestor import fetch_and_store_historical_klines
from .exchanges.base_interface import ExchangeInterface
from .exchanges.symbol_representation import SymbolRepresentation, FUTURE, SPOT, PERP


logger = logging.getLogger(__name__)


def get_target_symbols_from_local_db(
    db_conn,
    exchange_name_filter: Optional[str] = None,
    quote_asset_filter: Optional[str] = None,  # Standardized quote asset
    instrument_name_like: Optional[
        str
    ] = None,  # Exchange specific instrument name (supports SQL LIKE)
    specific_standard_symbols_list: Optional[
        List[str]
    ] = None,  # List of "BTC-USDT" strings
) -> List[Dict[str, Any]]:
    symbols_for_kline_fetch = []
    # To ensure we only add one DB entry per standard symbol when a specific list is provided
    processed_standard_symbols_for_list = set()

    with db_conn.cursor(cursor_factory=DictCursor) as cursor:
        # Base query using DISTINCT ON to pick one representative symbol
        # when multiple DB symbols might map to the same standard representation.
        # The ORDER BY clause for DISTINCT ON prioritizes more "direct" matches.
        query_select_part = """
            SELECT DISTINCT ON (s.base_asset, s.quote_asset, s.instrument_type, e.name)
                   s.symbol_id, s.instrument_name, s.base_asset, s.quote_asset, 
                   s.instrument_type, e.name as exchange_name
            FROM symbols s
            JOIN exchanges e ON s.exchange_id = e.exchange_id
        """
        # Order by clause for DISTINCT ON logic.
        # This tries to pick the "best" representative if multiple `instrument_name` map to the same standard.
        # For example, for standard BTC-USDT, prefer Binance's 'BTCUSDT' over a hypothetical 'BTCUSDT_OLD'.
        query_order_by_for_distinct = """
            ORDER BY s.base_asset, s.quote_asset, s.instrument_type, e.name,
                     (s.instrument_name = (s.base_asset || s.quote_asset)) DESC, /* Prefers direct concat like BTCUSDT */
                     (s.instrument_name = (s.base_asset || REPLACE(s.quote_asset, 'USD', 'USDT')) AND s.instrument_type = 'PERP' AND s.quote_asset = 'USD') DESC, /* Prefers BTCUSDT for BTC-USD-PERP */
                     LENGTH(s.instrument_name) ASC, /* Shorter instrument names often more primary */
                     s.instrument_name ASC /* Fallback to alphabetical */
        """

        conditions = []
        params: List[Any] = []

        if specific_standard_symbols_list:
            # If a specific list is given, we build a query to match exactly those standard symbols.
            # The DISTINCT ON will then pick the best single database entry for each.
            symbol_conditions_group = []
            for std_sym_str in specific_standard_symbols_list:
                try:
                    s_repr = SymbolRepresentation.parse(std_sym_str)
                    db_instrument_type_match = s_repr.instrument_type
                    if s_repr.instrument_type == FUTURE and s_repr.expiry_date:
                        db_instrument_type_match = f"{FUTURE}_{s_repr.expiry_date}"

                    # Conditions for one standard symbol: (base=? AND quote=? AND type=?)
                    condition_parts_for_one_symbol = [
                        "s.base_asset = %s",
                        "s.quote_asset = %s",
                        "s.instrument_type = %s",
                    ]
                    current_params_for_symbol = [
                        s_repr.base_asset,
                        s_repr.quote_asset,
                        db_instrument_type_match,
                    ]

                    if (
                        exchange_name_filter
                    ):  # Apply exchange filter if provided alongside the list
                        condition_parts_for_one_symbol.append("e.name = %s")
                        current_params_for_symbol.append(exchange_name_filter.lower())

                    symbol_conditions_group.append(
                        f"({' AND '.join(condition_parts_for_one_symbol)})"
                    )
                    params.extend(current_params_for_symbol)

                except ValueError as e:
                    logger.warning(
                        f"Could not parse standard symbol '{std_sym_str}' from --db-symbols-list for DB query: {e}"
                    )

            if symbol_conditions_group:
                conditions.append(
                    f"({' OR '.join(symbol_conditions_group)})"
                )  # OR groups for each symbol in list
            else:
                logger.warning(
                    "Empty or all invalid symbols in --db-symbols-list. No symbols will be fetched."
                )
                return []  # No valid symbols to query for

        else:  # General filters (not specific_standard_symbols_list)
            # DISTINCT ON will still apply, fetching one representative if multiple match general filters.
            if exchange_name_filter:
                conditions.append("e.name = %s")
                params.append(exchange_name_filter.lower())
            if quote_asset_filter:  # Filters on standardized quote_asset
                conditions.append("s.quote_asset = %s")
                params.append(quote_asset_filter.upper())
            if instrument_name_like:  # Filters on exchange-specific instrument_name
                conditions.append(
                    "s.instrument_name LIKE %s"
                )  # User provides wildcards e.g. %BTC%
                params.append(instrument_name_like.upper())

        final_query = query_select_part  # Start with SELECT DISTINCT ON
        if conditions:
            final_query += " WHERE " + " AND ".join(
                conditions
            )  # If list, this is OR'd groups of ANDs

        final_query += query_order_by_for_distinct  # Crucial: ORDER BY for DISTINCT ON

        try:
            # logger.debug(f"Executing query for target symbols: {cursor.mogrify(final_query, tuple(params)).decode()}")
            cursor.execute(final_query, tuple(params))

            for row in cursor.fetchall():
                # Reconstruct the standard symbol string from DB's standardized base, quote, type
                db_instrument_type = row[
                    "instrument_type"
                ]  # e.g., SPOT, PERP, FUTURE_YYMMDD
                s_repr_base_type = db_instrument_type
                s_repr_expiry = None

                if db_instrument_type.startswith(f"{FUTURE}_"):
                    parts = db_instrument_type.split("_", 1)
                    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 6:
                        s_repr_base_type = FUTURE  # Base type
                        s_repr_expiry = parts[1]  # Expiry date

                s_repr = SymbolRepresentation(
                    base_asset=row["base_asset"],
                    quote_asset=row["quote_asset"],
                    instrument_type=s_repr_base_type,
                    expiry_date=s_repr_expiry,
                )
                standard_symbol_for_task = (
                    s_repr.normalized
                )  # This is the "BTC-USDT" style string

                # If specific_standard_symbols_list was used, we ensure that only one
                # DB row is processed for each requested standard symbol.
                # The DISTINCT ON query should ideally handle this, but this is an extra safeguard
                # and ensures we only process symbols explicitly requested in the list.
                if specific_standard_symbols_list:
                    if standard_symbol_for_task not in specific_standard_symbols_list:
                        # The DISTINCT ON might pick a symbol whose standard form wasn't in the original request
                        # if the WHERE clause was too broad or OR'd. This check filters those out.
                        logger.debug(
                            f"Skipping symbol {standard_symbol_for_task} as it was not in the requested --db-symbols-list after DISTINCT ON selection."
                        )
                        continue
                    if standard_symbol_for_task in processed_standard_symbols_for_list:
                        logger.debug(
                            f"Already processed best match for standard symbol {standard_symbol_for_task}, skipping duplicate from DISTINCT ON."
                        )
                        continue
                    processed_standard_symbols_for_list.add(standard_symbol_for_task)

                symbols_for_kline_fetch.append(
                    {
                        "symbol_id": row["symbol_id"],
                        "exchange_instrument_name": row[
                            "instrument_name"
                        ],  # Exchange specific
                        "standard_symbol_str": standard_symbol_for_task,  # Standardized "BTC-USDT" for adapter
                        "exchange_name": row["exchange_name"],
                        "standard_base_asset": row["base_asset"],
                        "standard_quote_asset": row["quote_asset"],
                        "standard_instrument_type": row[
                            "instrument_type"
                        ],  # DB stored type (e.g. FUTURE_YYMMDD)
                    }
                )
        except psycopg2.Error as e:
            logger.error(f"DB Error fetching target symbols: {e}", exc_info=True)
            return []

    if not symbols_for_kline_fetch and (
        specific_standard_symbols_list
        or exchange_name_filter
        or quote_asset_filter
        or instrument_name_like
    ):
        logger.warning("No symbols matched the provided filters in the database.")
    elif specific_standard_symbols_list and len(symbols_for_kline_fetch) < len(
        specific_standard_symbols_list
    ):
        logger.warning(
            f"Found {len(symbols_for_kline_fetch)} symbols in DB out of {len(specific_standard_symbols_list)} requested in --db-symbols-list. Some might not exist or match filters."
        )

    logger.info(
        f"Selected {len(symbols_for_kline_fetch)} distinct symbols from local DB for kline fetching."
    )
    return symbols_for_kline_fetch


async def kline_fetch_worker_async(
    symbol_data: Dict[str, Any],
    config_path: str,
    start_date_cfg: str,
    end_date_cfg: Optional[str],
    interval_cfg: str,
    kline_db_chunk_size_cfg: int,
    stagger_delay_cfg: float,
    api_status_filter_cfg: str,
) -> Tuple[str, int, str]:  # symbol_display_name, count, status_message

    exchange_name = symbol_data["exchange_name"]
    standard_symbol_str_for_adapter = symbol_data[
        "standard_symbol_str"
    ]  # e.g. BTC-USDT
    exchange_instrument_name_for_logging = symbol_data[
        "exchange_instrument_name"
    ]  # e.g. BTCUSDT
    symbol_id = symbol_data["symbol_id"]

    symbol_display_name = f"{standard_symbol_str_for_adapter} (ExchInst: {exchange_instrument_name_for_logging}@{exchange_name}, DB ID: {symbol_id})"
    logger.debug(f"Worker INIT for {symbol_display_name}")

    worker_adapter: Optional[ExchangeInterface] = None
    worker_db_conn = None

    try:
        if stagger_delay_cfg > 0:
            await asyncio.sleep(stagger_delay_cfg)

        cfg_parser = load_config(config_path)
        worker_adapter = get_exchange_adapter(exchange_name, cfg_parser)
        if hasattr(
            worker_adapter, "_ensure_cache_populated"
        ):  # Essential for normalization
            await worker_adapter._ensure_cache_populated()

        worker_db_conn = get_db_connection(cfg_parser, new_instance=True)
        if worker_db_conn is None:
            logger.warning(
                f"Worker for {symbol_display_name}: FAILED to get DB connection."
            )
            return symbol_display_name, -1, "ERROR_DB_CONNECTION"

        proceed_with_fetch = True
        # Use exchange_instrument_name_for_logging for check_api_symbol_status as it's the exchange-specific name
        exchange_specific_symbol_for_api_check = symbol_data["exchange_instrument_name"]

        if api_status_filter_cfg.upper() != "NONE":
            status_info = await worker_adapter.check_api_symbol_status(
                exchange_specific_symbol_for_api_check
            )
            live_status = status_info.get("status", "UNKNOWN").upper()
            is_allowed_trading = status_info.get("is_trading_allowed", False)

            filter_type = "TRADING_ONLY"
            custom_statuses: List[str] = []
            if api_status_filter_cfg.upper().startswith("CUSTOM:"):
                filter_type = "CUSTOM"
                custom_statuses_part = api_status_filter_cfg.split("CUSTOM:", 1)[1]
                custom_statuses = [
                    s.strip().upper()
                    for s in custom_statuses_part.split(",")
                    if s.strip()
                ]
            elif api_status_filter_cfg.upper() == "ANY":
                filter_type = "ANY"

            if filter_type == "TRADING_ONLY" and not (
                live_status == "TRADING" and is_allowed_trading
            ):
                proceed_with_fetch = False
            elif filter_type == "CUSTOM" and live_status not in custom_statuses:
                proceed_with_fetch = False

            if not proceed_with_fetch:
                logger.info(
                    f"Worker for {symbol_display_name}: SKIPPED by API status filter (Live Status: {live_status}, Trading Allowed: {is_allowed_trading}, Filter: '{api_status_filter_cfg}')"
                )
                return symbol_display_name, 0, "SKIPPED_LIVE_CHECK"

        logger.debug(f"Worker for {symbol_display_name}: Fetching klines...")
        # Pass standard_symbol_str_for_adapter to fetch_and_store_historical_klines
        # Pass exchange_instrument_name_for_logging for more specific logging within that function
        count = await fetch_and_store_historical_klines(
            worker_adapter,
            worker_db_conn,
            symbol_id,
            standard_symbol_str_for_adapter,
            exchange_instrument_name_for_logging,
            interval_cfg,
            start_date_cfg,
            end_date_cfg,
            kline_db_chunk_size_cfg,
        )
        logger.debug(f"Worker for {symbol_display_name}: Processed ({count} klines)")
        return symbol_display_name, count, "PROCESSED"

    except Exception as e:
        logger.error(
            f"Error in kline worker for {symbol_display_name}: {type(e).__name__} - {e}",
            exc_info=True,
        )
        return symbol_display_name, -1, "ERROR_WORKER_EXCEPTION"
    finally:
        if worker_db_conn:
            worker_db_conn.close()
        if worker_adapter and hasattr(worker_adapter, "close_session"):
            await worker_adapter.close_session()
        logger.debug(f"Worker FINISHED for {symbol_display_name}")


async def main_fetch_klines_for_db_symbols_async():
    setup_logging()  # Initialize logging configuration
    parser = argparse.ArgumentParser(
        description="Fetches klines for symbols in the local DB using exchange adapters."
    )
    parser.add_argument(
        "--config", type=str, default=DEFAULT_CONFIG_PATH, help="Path to config file."
    )
    # DB Symbol Selection Filters
    parser.add_argument(
        "--db-exchange-name",
        type=str,
        help="Filter DB symbols by exchange_name (e.g., binance).",
    )
    parser.add_argument(
        "--db-quote-asset",
        type=str,
        help="Filter DB symbols by *standardized* quote_asset (e.g., USDT).",
    )
    parser.add_argument(
        "--db-instrument-like",
        type=str,
        help="Filter DB symbols by *exchange_instrument_name* LIKE (e.g., %%BTC%%). SQL wildcards needed.",
    )
    parser.add_argument(
        "--db-symbols-list",
        type=str,
        help="Comma-separated list of *standard* symbols from DB (e.g., BTC-USDT,ETH-USD-PERP). Overrides other DB filters if provided.",
    )

    # Kline Fetch Parameters
    parser.add_argument(
        "--start-date",
        type=str,
        help="Kline start date/datetime (UTC). Uses config default if not set.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="Kline end date/datetime (UTC). Default: fetch to latest.",
    )
    parser.add_argument(
        "--interval", type=str, help="Kline interval. Uses config default."
    )

    # Operational Parameters
    parser.add_argument(
        "--api-status-filter",
        type=str,
        help="Live API status check. Options: NONE, TRADING_ONLY, ANY, CUSTOM:STATUS1,STATUS2. Uses config default.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Max concurrent async workers. Uses config default.",
    )
    parser.add_argument(
        "--worker-delay",
        type=float,
        help="Staggered delay (s) per worker start. Uses config default.",
    )
    args = parser.parse_args()

    try:
        config_obj = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}", exc_info=True)
        exit(1)

    db_exchange_name_arg = args.db_exchange_name or config_obj.get(
        "ingestion", "default_db_exchange_filter", fallback=None
    )
    if db_exchange_name_arg == "":
        db_exchange_name_arg = None

    db_quote_asset_arg = args.db_quote_asset or config_obj.get(
        "ingestion", "default_db_quote_asset_filter", fallback=None
    )
    if db_quote_asset_arg == "":
        db_quote_asset_arg = None

    db_instrument_like_arg = args.db_instrument_like

    db_symbols_list_arg = (
        [s.strip().upper() for s in args.db_symbols_list.split(",")]
        if args.db_symbols_list
        else None
    )

    start_date_arg = args.start_date or config_obj.get(
        "ingestion", "default_start_date", fallback="2020-01-01T00:00:00Z"
    )
    end_date_arg = args.end_date
    interval_arg = args.interval or config_obj.get(
        "ingestion", "default_kline_interval", fallback="1m"
    )

    api_status_filter_arg = (
        args.api_status_filter
        or config_obj.get(
            "symbol_management",
            "default_kline_fetch_api_status_filter",
            fallback="TRADING_ONLY",
        )
    ).upper()

    max_workers_arg = (
        args.max_workers
        if args.max_workers is not None
        else config_obj.getint("ingestion", "max_workers", fallback=5)
    )
    worker_delay_arg = (
        args.worker_delay
        if args.worker_delay is not None
        else config_obj.getfloat("ingestion", "worker_symbol_delay", fallback=0.05)
    )

    kline_db_chunk_size = config_obj.getint(
        "settings", "kline_fetch_batch_size", fallback=500
    )

    logger.info("--- Kline Ingestion for DB Symbols (Adapter Based) ---")
    logger.info(f"Config Path: {args.config}")
    logger.info(
        f"DB Filters: Exchange='{db_exchange_name_arg or 'Any'}', QuoteAsset='{db_quote_asset_arg or 'Any'}', InstrumentLike='{db_instrument_like_arg or 'Any'}', SpecificList='{db_symbols_list_arg or 'None'}'"
    )
    logger.info(
        f"Kline Params: Start='{start_date_arg}', End='{end_date_arg or 'Latest'}', Interval='{interval_arg}'"
    )
    logger.info(
        f"Operational: APIStatusFilter='{api_status_filter_arg}', MaxWorkers={max_workers_arg}, WorkerDelay={worker_delay_arg}s, KlineDBChunkSize={kline_db_chunk_size}"
    )
    logger.info("---------------------------------------------")

    master_db_conn = None
    try:
        master_db_conn = get_db_connection(config_obj)
        if master_db_conn is None:
            logger.error("Failed to get master DB connection. Exiting.")
            return

        symbols_to_process = get_target_symbols_from_local_db(
            master_db_conn,
            db_exchange_name_arg,
            db_quote_asset_arg,
            db_instrument_like_arg,
            db_symbols_list_arg,
        )
        if not symbols_to_process:
            logger.warning(
                "No symbols found in local DB matching criteria. Nothing to fetch klines for."
            )
            return

        logger.info(
            f"Identified {len(symbols_to_process)} symbol tasks from DB for kline fetching."
        )
        # for i, s_data_log in enumerate(symbols_to_process): # Verbose, enable if needed
        #     logger.debug(f"  Task {i+1}: {s_data_log['standard_symbol_str']} ({s_data_log['exchange_instrument_name']}@{s_data_log['exchange_name']})")

        total_klines_ingested = 0
        processed_with_data_count = 0
        skipped_by_live_check_count = 0
        error_count = 0
        start_time_overall = time.monotonic()

        tasks = []
        current_stagger_delay = 0.0
        for s_data in symbols_to_process:
            task = kline_fetch_worker_async(
                s_data,
                args.config,
                start_date_arg,
                end_date_arg,
                interval_arg,
                kline_db_chunk_size,
                current_stagger_delay,
                api_status_filter_arg,
            )
            tasks.append(task)
            current_stagger_delay += worker_delay_arg

        semaphore = asyncio.Semaphore(max_workers_arg)

        async def sem_task_wrapper(task_coro):
            async with semaphore:
                return await task_coro

        results_processed: List[Any] = []
        for future in tqdm(
            asyncio.as_completed([sem_task_wrapper(task) for task in tasks]),
            total=len(tasks),
            desc="Processing Symbols Klines",
            unit="symbol",
        ):
            try:
                result = await future
                results_processed.append(result)
            except Exception as e_future:
                logger.error(
                    f"Worker task failed at semaphore/gather level: {e_future}",
                    exc_info=True,
                )
                results_processed.append(e_future)

        for res_item in results_processed:
            if isinstance(res_item, Exception):
                error_count += 1
                continue

            s_name_res, klines_fetched_res, status_msg_res = res_item
            if status_msg_res == "PROCESSED":
                if klines_fetched_res > 0:
                    total_klines_ingested += klines_fetched_res
                    processed_with_data_count += 1
            elif status_msg_res == "SKIPPED_LIVE_CHECK":
                skipped_by_live_check_count += 1
            elif status_msg_res.startswith("ERROR"):
                error_count += 1
            else:
                logger.warning(
                    f"Worker for '{s_name_res}' returned unexpected status: '{status_msg_res}' with {klines_fetched_res} klines."
                )

        end_time_overall = time.monotonic()
        logger.info("--- Kline Ingestion Summary (Adapter Based) ---")
        logger.info(f"Targeted symbol tasks from DB: {len(symbols_to_process)}")
        logger.info(
            f"Symbols processed with kline data fetched/updated: {processed_with_data_count}"
        )
        logger.info(
            f"Symbols skipped by live API status check: {skipped_by_live_check_count}"
        )
        logger.info(f"Symbols/tasks resulting in errors: {error_count}")
        logger.info(f"Total klines ingested/updated in DB: {total_klines_ingested}")
        logger.info(
            f"Total execution time: {end_time_overall - start_time_overall:.2f} seconds."
        )

    except Exception as e:
        logger.error(f"Main error in populate_db CLI: {e}", exc_info=True)
    finally:
        if master_db_conn:
            master_db_conn.close()
            logger.info("Master DB connection closed.")
        logger.info("Populate_db script finished.")


if __name__ == "__main__":
    asyncio.run(main_fetch_klines_for_db_symbols_async())
