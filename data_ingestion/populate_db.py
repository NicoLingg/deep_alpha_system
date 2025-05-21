import time
import asyncio
import argparse
import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd
from tqdm import tqdm
from typing import Optional, List, Dict, Any

from .utils import (
    load_config,
    get_db_connection,
    get_exchange_adapter,
    DEFAULT_CONFIG_PATH,
)
from .kline_ingestor import fetch_and_store_historical_klines
from .exchanges.base_interface import ExchangeInterface
from .exchanges.symbol_representation import (
    SymbolRepresentation,
)  # Import for type creation


def get_target_symbols_from_local_db(
    db_conn,
    exchange_name_filter: Optional[str] = None,
    quote_asset_filter: Optional[str] = None,
    instrument_name_like: Optional[str] = None,
    specific_symbols_list: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    symbols_for_kline_fetch = []
    with db_conn.cursor(cursor_factory=DictCursor) as cursor:
        query = """
            SELECT s.symbol_id, s.instrument_name, s.base_asset, s.quote_asset, s.instrument_type, e.name as exchange_name
            FROM symbols s
            JOIN exchanges e ON s.exchange_id = e.exchange_id
        """
        conditions = []
        params: List[Any] = []

        if specific_symbols_list:
            symbol_conditions = []
            for std_sym_str in specific_symbols_list:
                try:
                    s_repr = SymbolRepresentation.parse(std_sym_str)
                    condition_parts = ["s.base_asset = %s", "s.quote_asset = %s"]
                    params.extend([s_repr.base_asset, s_repr.quote_asset])

                    # Handle instrument type matching carefully
                    if s_repr.instrument_type == "SPOT":
                        condition_parts.append("s.instrument_type = %s")
                        params.append("SPOT")
                    elif s_repr.instrument_type == "PERP":
                        condition_parts.append("s.instrument_type = %s")
                        params.append("PERP")
                    elif s_repr.instrument_type == "FUTURE" and s_repr.expiry_date:
                        condition_parts.append("s.instrument_type = %s")
                        params.append(
                            f"FUTURE_{s_repr.expiry_date}"
                        )  # Match DB storage format
                    # Add option logic if needed
                    else:  # Generic type match
                        condition_parts.append("s.instrument_type = %s")
                        params.append(s_repr.instrument_type)

                    symbol_conditions.append(f"({' AND '.join(condition_parts)})")
                except ValueError as e:
                    print(
                        f"Warning: Could not parse standard symbol '{std_sym_str}' for DB query: {e}"
                    )
            if symbol_conditions:
                conditions.append(f"({' OR '.join(symbol_conditions)})")

        if exchange_name_filter:
            conditions.append("e.name = %s")
            params.append(exchange_name_filter.lower())
        if quote_asset_filter:
            conditions.append("s.quote_asset = %s")
            params.append(quote_asset_filter.upper())
        if instrument_name_like:
            conditions.append("s.instrument_name LIKE %s")
            params.append(instrument_name_like.upper())

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY e.name, s.base_asset, s.quote_asset, s.instrument_type"

        try:
            cursor.execute(query, tuple(params))
            for row in cursor.fetchall():
                s_repr = SymbolRepresentation(
                    base_asset=row["base_asset"],
                    quote_asset=row["quote_asset"],
                    instrument_type=row[
                        "instrument_type"
                    ],  # This is already FUTURE_YYMMDD from DB
                    # Expiry date parsing from instrument_type if it's FUTURE_YYMMDD
                    expiry_date=(
                        row["instrument_type"].split("_")[1]
                        if row["instrument_type"].startswith("FUTURE_")
                        else None
                    ),
                )
                standard_symbol = s_repr.normalized

                symbols_for_kline_fetch.append(
                    {
                        "symbol_id": row["symbol_id"],
                        "exchange_instrument_name": row["instrument_name"],
                        "standard_symbol_str": standard_symbol,  # This is the key for the adapter
                        "exchange_name": row["exchange_name"],
                        "standard_base_asset": row["base_asset"],
                        "standard_quote_asset": row["quote_asset"],
                        "standard_instrument_type": row["instrument_type"],
                    }
                )
        except psycopg2.Error as e:
            print(f"DB Error fetching target symbols: {e}")
            return []
    print(
        f"Selected {len(symbols_for_kline_fetch)} symbols from local DB based on filters for kline fetching."
    )
    return symbols_for_kline_fetch


async def kline_fetch_worker_async(
    symbol_data: Dict[str, Any],
    config_path: str,
    start_date: str,
    end_date: Optional[str],
    interval: str,
    batch_size: int,
    delay_per_worker: float,
    api_status_filter_config_str: str,
) -> Tuple[str, int, str]:
    exchange_name = symbol_data["exchange_name"]
    standard_symbol_str = symbol_data["standard_symbol_str"]
    exchange_instrument_name = symbol_data["exchange_instrument_name"]
    symbol_id = symbol_data["symbol_id"]
    symbol_display_name = (
        f"{standard_symbol_str} ({exchange_instrument_name}@{exchange_name})"
    )

    # print(f"[{symbol_display_name}] Worker: INIT") # Reduce verbosity
    worker_adapter: Optional[ExchangeInterface] = None
    worker_db_conn = None

    try:
        if delay_per_worker > 0:
            await asyncio.sleep(delay_per_worker)

        cfg = load_config(config_path)
        worker_adapter = get_exchange_adapter(exchange_name, cfg)
        # Ensure adapter's symbol cache is ready if it relies on it for internal ops
        if hasattr(worker_adapter, "_ensure_cache_populated"):
            await worker_adapter._ensure_cache_populated()

        worker_db_conn = get_db_connection(cfg, new_instance=True)
        if worker_db_conn is None:
            # print(f"[{symbol_display_name}] Worker: FAILED to get DB connection.") # Reduce verbosity
            return symbol_display_name, -1, "ERROR_DB_CONNECTION"

        proceed_with_fetch = True
        if api_status_filter_config_str.upper() != "NONE":
            status_info = await worker_adapter.check_api_symbol_status(
                exchange_instrument_name
            )
            live_status = status_info.get("status", "UNKNOWN").upper()
            is_allowed_trading = status_info.get("is_trading_allowed", False)

            filter_type = "TRADING_ONLY"
            custom_statuses: List[str] = []
            if api_status_filter_config_str.upper().startswith("CUSTOM:"):
                filter_type = "CUSTOM"
                custom_statuses_part = api_status_filter_config_str.split("CUSTOM:", 1)[
                    1
                ]
                custom_statuses = [
                    s.strip().upper()
                    for s in custom_statuses_part.split(",")
                    if s.strip()
                ]
            elif api_status_filter_config_str.upper() == "ANY":
                filter_type = "ANY"

            if filter_type == "TRADING_ONLY" and not (
                live_status == "TRADING" and is_allowed_trading
            ):
                proceed_with_fetch = False
            elif filter_type == "CUSTOM" and live_status not in custom_statuses:
                proceed_with_fetch = False

            if not proceed_with_fetch:
                # print(f"[{symbol_display_name}] Worker: SKIPPED_API_FILTER (Live Status: {live_status}, Trading Allowed: {is_allowed_trading}, Filter: '{api_status_filter_config_str}')") # Reduce verbosity
                return symbol_display_name, 0, "SKIPPED_LIVE_CHECK"

        # print(f"[{symbol_display_name}] Worker: FETCHING klines...") # Reduce verbosity
        count = await fetch_and_store_historical_klines(
            worker_adapter,
            worker_db_conn,
            symbol_id,
            standard_symbol_str,
            exchange_instrument_name,
            interval,
            start_date,
            end_date,
            batch_size,
        )
        # print(f"[{symbol_display_name}] Worker: PROCESSED ({count} klines)") # Reduce verbosity
        return symbol_display_name, count, "PROCESSED"
    except Exception as e:
        print(
            f"\n!!! ERROR in kline worker for {symbol_display_name}: {type(e).__name__} - {e}"
        )
        # import traceback; traceback.print_exc() # Verbose, enable if needed
        return symbol_display_name, -1, "ERROR"
    finally:
        if worker_db_conn:
            worker_db_conn.close()
        if worker_adapter and hasattr(worker_adapter, "close_session"):
            await worker_adapter.close_session()
        # print(f"[{symbol_display_name}] Worker: FINISHED") # Reduce verbosity


async def main_fetch_klines_for_db_symbols_async():
    parser = argparse.ArgumentParser(
        description="Fetches klines for symbols in the local DB using exchange adapters."
    )
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG_PATH)
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
        help="Filter DB symbols by exchange_instrument_name LIKE (e.g., %%BTC%%). SQL wildcards needed.",
    )
    parser.add_argument(
        "--db-symbols-list",
        type=str,
        help="Comma-separated list of *standard* symbols from DB (e.g., BTC-USDT,ETH-USDT-PERP).",
    )
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

    config_obj = load_config(args.config)
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
    max_workers_arg = args.max_workers or config_obj.getint(
        "ingestion", "max_workers", fallback=5
    )
    worker_delay_arg = (
        args.worker_delay
        if args.worker_delay is not None
        else config_obj.getfloat("ingestion", "worker_symbol_delay", fallback=0.05)
    )
    kline_db_batch_size = config_obj.getint(
        "settings", "kline_fetch_batch_size", fallback=500
    )

    print(
        "--- Kline Ingestion for DB Symbols (Adapter Based) ---"
    )  # ... (rest of print statements)
    print(
        f"DB Filters: Exchange='{db_exchange_name_arg or 'Any'}', QuoteAsset='{db_quote_asset_arg or 'Any'}', InstrumentLike='{db_instrument_like_arg or 'Any'}', SpecificList='{db_symbols_list_arg or 'None'}'"
    )
    print(
        f"Kline Params: Start='{start_date_arg}', End='{end_date_arg or 'Latest'}', Interval='{interval_arg}'"
    )
    print(f"Live API Status Check Before Fetch: '{api_status_filter_arg}'")
    print(
        f"Concurrency: MaxWorkers={max_workers_arg}, WorkerStaggerDelay={worker_delay_arg}s, KlineBatchSize={kline_db_batch_size}"
    )
    print("---------------------------------------------")

    master_db_conn = None
    try:
        master_db_conn = get_db_connection(config_obj)
        if master_db_conn is None:
            print("Failed to get master DB connection. Exiting.")
            return

        symbols_to_process = get_target_symbols_from_local_db(
            master_db_conn,
            db_exchange_name_arg,
            db_quote_asset_arg,
            db_instrument_like_arg,
            db_symbols_list_arg,
        )
        if not symbols_to_process:
            print(
                "No symbols found in local DB matching criteria. Nothing to fetch klines for."
            )
            return

        print(
            f"Identified {len(symbols_to_process)} symbol tasks from DB for kline fetching."
        )
        # for i, s_data_log in enumerate(symbols_to_process): # Reduce verbosity
        #     print(f"  Task {i+1}: {s_data_log['standard_symbol_str']} ({s_data_log['exchange_instrument_name']}@{s_data_log['exchange_name']})")

        total_klines_ingested = 0
        processed_with_data_count = 0
        skipped_by_live_check_count = 0
        error_count = 0
        start_time_overall = time.monotonic()
        tasks = []
        current_delay = 0.0
        for s_data in symbols_to_process:
            task = kline_fetch_worker_async(
                s_data,
                args.config,
                start_date_arg,
                end_date_arg,
                interval_arg,
                kline_db_batch_size,
                current_delay,
                api_status_filter_arg,
            )
            tasks.append(task)
            current_delay += worker_delay_arg

        semaphore = asyncio.Semaphore(max_workers_arg)

        async def sem_task_wrapper(task_coro):
            async with semaphore:
                return await task_coro

        results_processed: List[Any] = []
        for future in tqdm(
            asyncio.as_completed([sem_task_wrapper(task) for task in tasks]),
            total=len(tasks),
            desc="Processing Symbols",
        ):
            try:
                result = await future
                results_processed.append(result)
            except Exception as e_future:
                results_processed.append(e_future)

        for res_item in results_processed:
            if isinstance(res_item, Exception):
                error_count += 1
                print(f"\n!!! Worker task failed at gather/semaphore level: {res_item}")
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

        end_time_overall = time.monotonic()
        print("\n--- Kline Ingestion Summary (Adapter Based) ---")
        print(f"Targeted symbol tasks from DB: {len(symbols_to_process)}")
        print(
            f"Symbols processed with kline data fetched/updated: {processed_with_data_count}"
        )
        print(
            f"Symbols skipped by live API status check: {skipped_by_live_check_count}"
        )
        print(f"Symbols/tasks resulting in errors: {error_count}")
        print(f"Total klines ingested/updated in DB: {total_klines_ingested}")
        print(
            f"Total execution time: {end_time_overall - start_time_overall:.2f} seconds."
        )

    except Exception as e:
        print(f"Main error in populate_db: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if master_db_conn:
            master_db_conn.close()
            print("Master DB connection closed.")


if __name__ == "__main__":
    asyncio.run(main_fetch_klines_for_db_symbols_async())
