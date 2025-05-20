import time
import asyncio
import argparse
import psycopg2
import threading
import concurrent.futures

from tqdm import tqdm
from binance.client import Client
from binance.exceptions import BinanceAPIException


from .utils import (
    load_config,
    get_db_connection,
    get_binance_client,
    DEFAULT_CONFIG_PATH,
)
from .kline_ingestor import fetch_and_store_historical_klines


def get_target_symbols_from_local_db(
    db_conn,
    quote_asset_filter=None,
    instrument_name_like=None,
    specific_symbols_list=None,
):
    """
    Fetches symbol_id and instrument_name from the local database based on various filters.
    """
    symbols_for_kline_fetch = []
    with db_conn.cursor() as cursor:
        query = "SELECT symbol_id, instrument_name FROM symbols"
        conditions = []
        params = []

        if specific_symbols_list:
            if not isinstance(specific_symbols_list, list):
                specific_symbols_list = [specific_symbols_list]
            if not specific_symbols_list:  # Empty list after processing
                print(
                    "Warning: --db-symbols-list provided but was empty after parsing."
                )
            else:
                placeholders = ", ".join(["%s"] * len(specific_symbols_list))
                conditions.append(f"instrument_name IN ({placeholders})")
                params.extend(specific_symbols_list)
        else:  # Only apply these if specific_symbols_list is not used
            if quote_asset_filter:
                conditions.append("quote_asset = %s")
                params.append(quote_asset_filter)
            if instrument_name_like:
                conditions.append("instrument_name LIKE %s")
                params.append(instrument_name_like)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY instrument_name"

        try:
            cursor.execute(query, tuple(params))
            for row in cursor.fetchall():
                symbols_for_kline_fetch.append(
                    {
                        "symbol_id": row["symbol_id"],
                        "instrument_name": row["instrument_name"],
                    }
                )
        except psycopg2.Error as e:
            print(f"DB Error fetching target symbols: {e}")
            return []  # Return empty list on error

    print(
        f"Selected {len(symbols_for_kline_fetch)} symbols from local DB based on filters for kline fetching."
    )
    return symbols_for_kline_fetch


def check_live_api_status(
    b_client,
    instrument_name,
    allowed_statuses_if_custom=None,
    default_to_trading_spot=True,
):
    """
    Checks the live status of a symbol on Binance.
    Returns True if condition met to proceed, False otherwise.
    """
    try:
        s_info = b_client.get_symbol_info(instrument_name)
        if not s_info:
            # This print might be too verbose if many symbols are checked. Consider logging.
            # print(f"[{instrument_name}] Live check: get_symbol_info returned None (unexpected).")
            return False

        current_status = s_info.get("status")
        # isSpotTradingAllowed is preferred if available, otherwise check permissions array
        is_spot = s_info.get(
            "isSpotTradingAllowed", "SPOT" in s_info.get("permissions", [])
        )

        if allowed_statuses_if_custom:
            return current_status in allowed_statuses_if_custom
        elif default_to_trading_spot:
            return current_status == "TRADING" and is_spot
        else:
            return True

    except BinanceAPIException as e:
        # These prints can also be verbose in a loop.
        # if e.code == -1121:
        #     print(f"[{instrument_name}] Live check: Symbol not found on Binance (delisted?). Code: {e.code}")
        # else:
        #     print(f"[{instrument_name}] Live check: API error during status check: {e}. Code: {e.code}")
        return False  # Treat API errors during check as "do not proceed"
    except Exception:  # Catch any other unexpected error
        # print(f"[{instrument_name}] Live check: Unexpected error during status check: {e}")
        return False


def kline_fetch_worker(
    symbol_data,
    config_path,
    start_date,
    end_date,
    interval,
    batch_size,
    delay,
    api_status_filter_config_str,
):
    instrument_name = symbol_data["instrument_name"]
    symbol_id = symbol_data["symbol_id"]

    try:
        current_loop = asyncio.get_event_loop()
        if (
            current_loop.is_closed()
        ):  
            raise RuntimeError("Event loop is closed")
    except (
        RuntimeError
    ) as e:  
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)

    worker_b_client = None
    worker_db_conn = None
    tqdm_status_postfix = {"symbol": instrument_name, "status": "INIT"}

    try:
        cfg = load_config(config_path)
        worker_b_client = get_binance_client(cfg)  # Each worker gets its own client
        worker_db_conn = get_db_connection(
            cfg
        )  # Each worker gets its own DB connection

        if delay > 0:
            time.sleep(delay)

        tqdm_status_postfix["status"] = "LIVE_CHECK"
        proceed_with_fetch = True
        if api_status_filter_config_str != "NONE":
            custom_statuses = None
            check_default_trading_spot = True

            if api_status_filter_config_str.startswith("CUSTOM:"):
                custom_statuses_part = api_status_filter_config_str.split("CUSTOM:", 1)[
                    1
                ]
                custom_statuses = [
                    s.strip().upper()
                    for s in custom_statuses_part.split(",")
                    if s.strip()
                ]
                check_default_trading_spot = False
            elif api_status_filter_config_str == "TRADING_SPOT_ONLY":
                pass

            is_allowed = check_live_api_status(
                worker_b_client,
                instrument_name,
                allowed_statuses_if_custom=custom_statuses,
                default_to_trading_spot=check_default_trading_spot,
            )
            if not is_allowed:
                proceed_with_fetch = False
                tqdm_status_postfix["status"] = "SKIPPED_API_FILTER"

        if proceed_with_fetch:
            tqdm_status_postfix["status"] = "FETCHING"
            count = fetch_and_store_historical_klines(
                worker_b_client,
                worker_db_conn,
                symbol_id,
                instrument_name,
                interval,
                start_date,
                end_date,
                batch_size,
            )
            tqdm_status_postfix["status"] = f"PROCESSED ({count} klines)"
            return instrument_name, count, "PROCESSED", tqdm_status_postfix
        else:
            return instrument_name, 0, "SKIPPED_LIVE_CHECK", tqdm_status_postfix

    except Exception as e:
        # Print concise error for the worker, more details can be logged if using logging module
        # Added thread name for better debugging context.
        print(
            f"\n!!! ERROR in kline worker for {instrument_name} (Thread: {threading.current_thread().name}): {type(e).__name__} - {e}"
        )
        # import traceback # Uncomment for full traceback from the thread
        # print(traceback.format_exc()) # Uncomment for full traceback
        tqdm_status_postfix["status"] = "ERROR"
        return instrument_name, -1, "ERROR", tqdm_status_postfix
    finally:
        if worker_db_conn:
            worker_db_conn.close()
        # Note: Do not close the asyncio loop here if threads are reused by ThreadPoolExecutor,
        # as it might be needed by subsequent tasks on the same thread.
        # Asyncio loops set via asyncio.set_event_loop() are generally managed per thread.


def main_fetch_klines_for_db_symbols():
    parser = argparse.ArgumentParser(
        description="Stage 2: Fetches klines for symbols ALREADY IN THE LOCAL DATABASE."
    )
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG_PATH)
    parser.add_argument(
        "--db-quote-asset",
        type=str,
        help="Filter DB symbols by quote_asset (e.g., USDT).",
    )
    parser.add_argument(
        "--db-instrument-like",
        type=str,
        help="Filter DB symbols by instrument_name LIKE (e.g., %%BTC%%).",
    )
    parser.add_argument(
        "--db-symbols-list",
        type=str,
        help="Comma-separated list of instrument_names from DB (e.g., BTCUSDT,ETHUSDT).",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Kline start date/datetime (UTC, e.g., '2023-01-01' or '2023-01-01 00:00:00'). Uses config default if not set.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="Kline end date/datetime (UTC, e.g., '2023-01-31' or '2023-01-31 23:59:59'). Default: fetch to latest.",
    )
    parser.add_argument(
        "--interval", type=str, help="Kline interval. Uses config default."
    )
    parser.add_argument(
        "--api-status-filter",
        type=str,
        help="Live API status check. Options: NONE, TRADING_SPOT_ONLY, CUSTOM:STATUS1,STATUS2. Uses config default.",
    )
    parser.add_argument(
        "--max-workers", type=int, help="Max concurrent workers. Uses config default."
    )
    parser.add_argument(
        "--worker-delay", type=float, help="Delay (s) per worker. Uses config default."
    )
    args = parser.parse_args()

    config_obj = load_config(args.config)

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
        "ingestion", "default_start_date", fallback="2020-01-01"
    )
    end_date_arg = args.end_date
    interval_arg = args.interval or config_obj.get(
        "ingestion", "default_kline_interval", fallback=Client.KLINE_INTERVAL_1MINUTE
    )
    api_status_filter_arg = args.api_status_filter or config_obj.get(
        "symbol_management",
        "default_kline_fetch_api_status_filter",
        fallback="TRADING_SPOT_ONLY",
    )
    max_workers_arg = args.max_workers or config_obj.getint(
        "ingestion", "max_workers", fallback=5
    )
    worker_delay_arg = (
        args.worker_delay
        if args.worker_delay is not None
        else config_obj.getfloat("ingestion", "worker_symbol_delay", fallback=0.5)
    )
    kline_db_batch_size = config_obj.getint(
        "settings", "kline_fetch_batch_size", fallback=500
    )

    print("--- Stage 2: Kline Ingestion for DB Symbols ---")
    print(f"Config: {args.config}")
    print(
        f"DB Filters: QuoteAsset='{db_quote_asset_arg or 'Any'}', InstrumentLike='{db_instrument_like_arg or 'Any'}', SpecificList='{db_symbols_list_arg or 'None'}'"
    )
    print(
        f"Kline Params: Start='{start_date_arg}', End='{end_date_arg or 'Latest'}', Interval='{interval_arg}'"
    )
    print(f"Live API Status Check Before Fetch: '{api_status_filter_arg}'")
    print(
        f"Concurrency: MaxWorkers={max_workers_arg}, WorkerDelay={worker_delay_arg}s, BatchSize={kline_db_batch_size}"
    )
    print("---------------------------------------------")

    master_db_conn = None
    try:
        master_db_conn = get_db_connection(config_obj)
        symbols_to_process = get_target_symbols_from_local_db(
            master_db_conn,
            quote_asset_filter=db_quote_asset_arg,
            instrument_name_like=db_instrument_like_arg,
            specific_symbols_list=db_symbols_list_arg,
        )

        if not symbols_to_process:
            print(
                "No symbols found in local DB matching criteria. Nothing to fetch klines for."
            )
            return

        print(
            f"Identified {len(symbols_to_process)} symbols from DB for kline fetching."
        )
        if 0 < len(symbols_to_process) <= 10:
            print(f"Symbols: {[s['instrument_name'] for s in symbols_to_process]}")
        elif len(symbols_to_process) > 10:
            print(
                f"First 10: {[s['instrument_name'] for s in symbols_to_process[:10]]}..."
            )

        total_klines_ingested = 0
        processed_with_data_count = 0
        skipped_by_live_check_count = 0
        error_count = 0
        start_time_overall = time.time()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers_arg
        ) as executor:
            future_to_symbol_map = {
                executor.submit(
                    kline_fetch_worker,
                    s_data,
                    args.config,
                    start_date_arg,
                    end_date_arg,
                    interval_arg,
                    kline_db_batch_size,
                    worker_delay_arg,
                    api_status_filter_arg,
                ): s_data["instrument_name"]
                for s_data in symbols_to_process
            }

            pbar_symbols = tqdm(
                total=len(symbols_to_process), desc="Processing Symbols", unit="symbol"
            )

            for future in concurrent.futures.as_completed(future_to_symbol_map):
                instrument_name = future_to_symbol_map[future]
                try:
                    _s_name_res, klines_fetched_res, status_msg_res, worker_postfix = (
                        future.result()
                    )
                    pbar_symbols.set_postfix(worker_postfix, refresh=True)

                    if status_msg_res == "PROCESSED":
                        total_klines_ingested += klines_fetched_res
                        if klines_fetched_res > 0:
                            processed_with_data_count += 1
                    elif status_msg_res == "SKIPPED_LIVE_CHECK":
                        skipped_by_live_check_count += 1
                    elif status_msg_res == "ERROR":
                        error_count += 1
                except Exception as exc:
                    error_count += 1
                    print(
                        f"\n!!! EXCEPTION processing future for {instrument_name}: {type(exc).__name__} - {exc}"
                    )
                pbar_symbols.update(1)
            pbar_symbols.close()

        end_time_overall = time.time()
        print()
        print("\n--- Kline Ingestion Summary (from DB list) ---")
        print(f"Targeted symbols from DB: {len(symbols_to_process)}")
        print(f"Symbols with klines fetched/updated: {processed_with_data_count}")
        print(
            f"Symbols skipped by live API status check: {skipped_by_live_check_count}"
        )
        print(f"Symbols resulting in errors during kline fetch worker: {error_count}")
        print(f"Total klines ingested/updated: {total_klines_ingested}")
        print(
            f"Total execution time: {end_time_overall - start_time_overall:.2f} seconds."
        )

    except Exception as e:
        print(f"Main error in populate_db (Stage 2): {e}")
        import traceback

        traceback.print_exc()
    finally:
        if master_db_conn:
            master_db_conn.close()
            print("Main DB connection closed.")


if __name__ == "__main__":
    main_fetch_klines_for_db_symbols()
