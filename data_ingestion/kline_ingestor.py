import time
import psycopg2
import argparse
import pandas as pd
from psycopg2.extras import execute_values
from binance.client import Client
from binance.exceptions import BinanceAPIException
from tqdm import tqdm
from datetime import datetime as dt, timezone, timedelta  # timedelta for interval math

from .utils import (
    load_config,
    get_db_connection,
    get_binance_client,
    get_or_create_exchange_id,
    get_or_create_symbol_id,
    DEFAULT_CONFIG_PATH,
)


def store_klines_batch(conn, klines_data, symbol_id, interval, batch_size=500):
    if not klines_data:
        return 0
    records_to_insert = []
    for kline in klines_data:
        try:
            records_to_insert.append(
                (
                    pd.to_datetime(kline[0], unit="ms"),
                    symbol_id,
                    interval,
                    float(kline[1]),
                    float(kline[2]),
                    float(kline[3]),
                    float(kline[4]),
                    float(kline[5]),
                    pd.to_datetime(kline[6], unit="ms"),
                    float(kline[7]),
                    int(kline[8]),
                    float(kline[9]),
                    float(kline[10]),
                )
            )
        except (IndexError, ValueError) as e:
            print(
                f"\nWarning: Skipping malformed kline data for symbol_id {symbol_id}: {kline}. Error: {e}"
            )
            continue
    if not records_to_insert:
        return 0
    query = """
        INSERT INTO klines (time, symbol_id, interval, open_price, high_price, low_price, close_price,
                            volume, close_time, quote_asset_volume, number_of_trades,
                            taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
        VALUES %s
        ON CONFLICT (time, symbol_id, interval) DO UPDATE SET
            open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume, close_time = EXCLUDED.close_time,
            quote_asset_volume = EXCLUDED.quote_asset_volume,
            number_of_trades = EXCLUDED.number_of_trades,
            taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
            ingested_at = CURRENT_TIMESTAMP;
    """
    inserted_count_for_batch = 0
    with conn.cursor() as cursor:
        try:
            execute_values(cursor, query, records_to_insert, page_size=batch_size)
            conn.commit()
            inserted_count_for_batch = len(records_to_insert)
        except psycopg2.Error as e:
            conn.rollback()
            print(f"\nDB error inserting klines for symbol_id {symbol_id}: {e}")
            return 0
    return inserted_count_for_batch


def get_local_kline_daterange(db_conn, symbol_id, interval):
    min_time_local, max_time_local = None, None
    try:
        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT MIN(time AT TIME ZONE 'UTC') as min_db_time, MAX(time AT TIME ZONE 'UTC') as max_db_time
                FROM klines
                WHERE symbol_id = %s AND interval = %s
                """,
                (symbol_id, interval),
            )
            result = cursor.fetchone()
            if result and result["min_db_time"] is not None:
                min_time_local = result["min_db_time"].replace(tzinfo=timezone.utc)
                max_time_local = result["max_db_time"].replace(tzinfo=timezone.utc)
    except psycopg2.Error as e:
        print(f"\nDB Error checking local kline range for symbol_id {symbol_id}: {e}")
    return min_time_local, max_time_local


def get_interval_timedelta(interval_str: str) -> timedelta:
    """Converts Binance interval string to timedelta. Simplified."""
    if interval_str.endswith("m"):
        return timedelta(minutes=int(interval_str[:-1]))
    elif interval_str.endswith("h"):
        return timedelta(hours=int(interval_str[:-1]))
    elif interval_str.endswith("d"):
        return timedelta(days=int(interval_str[:-1]))
    elif interval_str.endswith("w"):
        return timedelta(weeks=int(interval_str[:-1]))
    raise ValueError(f"Unsupported interval string for timedelta: {interval_str}")


def fetch_and_store_historical_klines(
    b_client,
    db_conn,
    symbol_id,
    instrument_name,
    interval_str,  # e.g., "1m", "1h"
    requested_start_str_dt,  # Expects "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"
    requested_end_str_dt=None,  # Expects "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS", or None
    kline_batch_size=500,
):
    total_processed_for_symbol_run = 0

    # --- 1. Parse and Validate Requested Dates ---
    try:
        # Ensure start_date is parsed as UTC
        req_start_dt = (
            pd.to_datetime(requested_start_str_dt, errors="raise").tz_localize("UTC")
            if pd.to_datetime(requested_start_str_dt, errors="raise").tzinfo is None
            else pd.to_datetime(requested_start_str_dt, errors="raise").tz_convert(
                "UTC"
            )
        )
    except Exception as e:
        print(
            f"\n[{instrument_name}] Invalid start_date format: '{requested_start_str_dt}'. Must be like 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'. Error: {e}"
        )
        return 0

    req_end_dt = None
    if requested_end_str_dt:
        try:
            req_end_dt = (
                pd.to_datetime(requested_end_str_dt, errors="raise").tz_localize("UTC")
                if pd.to_datetime(requested_end_str_dt, errors="raise").tzinfo is None
                else pd.to_datetime(requested_end_str_dt, errors="raise").tz_convert(
                    "UTC"
                )
            )
            if req_end_dt < req_start_dt:
                print(
                    f"\n[{instrument_name}] Requested end_date '{requested_end_str_dt}' is before start_date '{requested_start_str_dt}'. Skipping."
                )
                return 0
        except Exception as e:
            print(
                f"\n[{instrument_name}] Invalid end_date format: '{requested_end_str_dt}'. Must be like 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'. Error: {e}"
            )
            return 0

    print(
        f"\n[{instrument_name}] Parsed request: Fetch from '{req_start_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}' to '{req_end_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if req_end_dt else 'latest'}' for interval '{interval_str}'."
    )

    # --- 2. Get Local Data Range ---
    min_time_local, max_time_local = get_local_kline_daterange(
        db_conn, symbol_id, interval_str
    )
    interval_delta = get_interval_timedelta(
        interval_str
    )  # For precise next/prev kline time

    if min_time_local:
        print(
            f"[{instrument_name}] Local data found: {min_time_local.strftime('%Y-%m-%d %H:%M:%S %Z')} to {max_time_local.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )
    else:
        print(f"[{instrument_name}] No local data found for this symbol/interval.")

    # --- 3. Determine Fetch Tasks ---
    fetch_tasks = []  # List of tuples: (api_start_str, api_end_str, task_description)

    # Default end for API if req_end_dt is None (fetch to latest)
    api_req_end_str = req_end_dt.strftime("%Y-%m-%d %H:%M:%S") if req_end_dt else None

    if not min_time_local:  # No local data at all
        print(
            f"[{instrument_name}] Strategy: No local data. Fetching full requested range."
        )
        fetch_tasks.append(
            (
                req_start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                api_req_end_str,
                "full range (no local data)",
            )
        )
    else:
        # Local data exists, determine gaps or new data needed

        # Task 1: Backfill (Fetch older data if requested start is before local min)
        if req_start_dt < min_time_local:
            # Fetch from requested_start up to (but not including) min_time_local
            backfill_end_dt = (
                min_time_local - interval_delta
            )  # End of kline *before* min_time_local
            # Ensure backfill_end is not before req_start_dt
            if backfill_end_dt >= req_start_dt:
                print(f"[{instrument_name}] Strategy: Backfilling older data.")
                fetch_tasks.append(
                    (
                        req_start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        backfill_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "older data (backfill)",
                    )
                )
            else:  # Requested start is very close to min_time_local, no separate backfill task needed
                pass

        # Task 2: Forward-fill (Fetch newer data if requested end is after local max, or if fetching to latest)
        # Start fetching from the kline *at* max_time_local (ON CONFLICT handles it)
        # or from req_start_dt if req_start_dt is later than max_time_local (filling a middle gap)

        start_for_newer_data_dt = max(max_time_local, req_start_dt)

        if req_end_dt:  # Fixed requested end date
            if (
                req_end_dt > max_time_local
            ):  # Only if requested end is actually after what we have
                # Also ensure we don't try to fetch if start_for_newer_data_dt is already >= req_end_dt
                if start_for_newer_data_dt < req_end_dt:
                    print(
                        f"[{instrument_name}] Strategy: Fetching newer data up to fixed end date."
                    )
                    fetch_tasks.append(
                        (
                            start_for_newer_data_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            req_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            "newer data (to fixed end)",
                        )
                    )
                else:
                    print(
                        f"[{instrument_name}] Strategy: Requested fixed end date '{req_end_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}' is already covered or not after effective start. No newer data fetch needed for this segment."
                    )
            else:  # Requested end date is covered by local data
                print(
                    f"[{instrument_name}] Strategy: Requested fixed end date '{req_end_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}' is within or before local max time. No newer data fetch needed for this segment."
                )
        else:  # Fetching to latest (req_end_dt is None)
            # Always try to fetch from start_for_newer_data_dt to latest
            # The API will return few/no klines if already up-to-date.
            print(f"[{instrument_name}] Strategy: Fetching newer data up to latest.")
            fetch_tasks.append(
                (
                    start_for_newer_data_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    None,  # API end_str = None for latest
                    "newer data (to latest)",
                )
            )

    if not fetch_tasks:
        print(
            f"[{instrument_name}] All data for the requested range [{req_start_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} - {req_end_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if req_end_dt else 'latest'}] seems to be locally available. No API calls will be made."
        )
        return 0

    # --- 4. Execute Fetch Tasks ---
    for task_idx, (
        effective_api_start_str,
        effective_api_end_str,
        task_desc,
    ) in enumerate(fetch_tasks):
        print(
            f"[{instrument_name}] Executing Task {task_idx+1}/{len(fetch_tasks)} ({task_desc}): API From '{effective_api_start_str}' to '{effective_api_end_str or 'latest'}'"
        )

        api_klines_retrieved_this_task = 0
        klines_processed_this_task = 0

        try:
            klines_generator = b_client.get_historical_klines_generator(
                instrument_name,
                interval_str,
                effective_api_start_str,
                end_str=effective_api_end_str,
            )

            batch = []
            with tqdm(
                desc=f"[{instrument_name}] Task {task_idx+1} DL",
                unit=" klines",
                leave=False,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
            ) as pbar_task_dl:
                for kline in klines_generator:
                    api_klines_retrieved_this_task += 1
                    pbar_task_dl.update(1)  # No total, so it acts as a counter
                    batch.append(kline)

                    if len(batch) >= kline_batch_size:
                        processed_in_batch = store_klines_batch(
                            db_conn, batch, symbol_id, interval_str, kline_batch_size
                        )
                        klines_processed_this_task += processed_in_batch
                        batch = []
                        if processed_in_batch > 0:
                            time.sleep(0.05)

                if batch:
                    processed_in_batch = store_klines_batch(
                        db_conn, batch, symbol_id, interval_str, kline_batch_size
                    )
                    klines_processed_this_task += processed_in_batch

            total_processed_for_symbol_run += klines_processed_this_task

            if (
                api_klines_retrieved_this_task == 0
                and effective_api_start_str != effective_api_end_str
            ):
                print(
                    f"[{instrument_name}] Task {task_idx+1} ({task_desc}): No klines returned from API."
                )
            elif api_klines_retrieved_this_task > 0:
                print(
                    f"[{instrument_name}] Task {task_idx+1} ({task_desc}): API: {api_klines_retrieved_this_task} klines, DB Processed: {klines_processed_this_task}."
                )

        except BinanceAPIException as e:
            print()
            error_msg = f"API Error (Task {task_idx+1}, {task_desc}) for {instrument_name} API range ['{effective_api_start_str}' to '{effective_api_end_str}']: {e.code} - {e.message}"
            if e.code == -1121:
                error_msg = f"API Error (Task {task_idx+1}, {task_desc}) for {instrument_name}: Invalid symbol. (Code: {e.code})"
            elif "Invalid date range" in str(e.message) or e.code == -1003:
                error_msg = f"API Error (Task {task_idx+1}, {task_desc}) for {instrument_name}: Invalid API date range/param or too much data. (Code: {e.code})"
            print(error_msg)
            continue
        except Exception as e:
            print()
            print(
                f"Unexpected error (Task {task_idx+1}, {task_desc}) for {instrument_name} API range ['{effective_api_start_str}' to '{effective_api_end_str}']: {e}."
            )
            import traceback

            traceback.print_exc()
            continue

    if total_processed_for_symbol_run > 0:
        print(
            f"[{instrument_name}] Completed all tasks. Total DB klines processed/updated in this run: {total_processed_for_symbol_run}."
        )
    else:
        print(
            f"[{instrument_name}] Completed all tasks. No new klines were inserted/updated in DB for this run (data may be up-to-date or API returned no data for requested ranges)."
        )

    return total_processed_for_symbol_run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LEGACY: Download klines for a SINGLE symbol (expects specific date formats). Use populate_db.py for bulk."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config file (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--symbol", type=str, required=True, help="Trading symbol (e.g., BTCUSDT)."
    )
    parser.add_argument(
        "--base_asset",
        type=str,
        default=None,
        help="Base asset (e.g., BTC). For new symbol creation.",
    )
    parser.add_argument(
        "--quote_asset",
        type=str,
        default=None,
        help="Quote asset (e.g., USDT). For new symbol creation.",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default=Client.KLINE_INTERVAL_1MINUTE,
        help=f"Kline interval (e.g., 1m, 5m, 1h). Default: {Client.KLINE_INTERVAL_1MINUTE}.",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        required=True,
        help="Kline start date/datetime (UTC, e.g., '2023-01-01' or '2023-01-01 00:00:00').",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="Kline end date/datetime (UTC, e.g., '2023-01-31' or '2023-01-31 23:59:59'). Default: fetch to latest.",
    )
    args = parser.parse_args()

    try:
        config_object = load_config(args.config)
    except FileNotFoundError as e:
        print(e)
        exit(1)
    except KeyError as e:
        print(f"Configuration error: {e}")
        exit(1)

    kline_db_batch_size = int(
        config_object.get("settings", "kline_fetch_batch_size", fallback=500)
    )
    db_connection = None
    try:
        db_connection = get_db_connection(config_object)
        binance_client = get_binance_client(config_object)
        print(
            "Successfully connected to DB and Binance client for single symbol fetch."
        )

        with db_connection.cursor() as cursor:
            exchange_id = get_or_create_exchange_id(cursor, "Binance")
            symbol_id = get_or_create_symbol_id(
                cursor,
                exchange_id,
                args.symbol.upper(),
                args.base_asset.upper() if args.base_asset else None,
                args.quote_asset.upper() if args.quote_asset else None,
            )
            db_connection.commit()

        if symbol_id is None:
            print(f"Could not process symbol {args.symbol.upper()} in DB. Exiting.")
            exit(1)

        print(f"Targeting Symbol: {args.symbol.upper()} (ID: {symbol_id})")

        fetch_and_store_historical_klines(
            binance_client,
            db_connection,
            symbol_id,
            args.symbol.upper(),
            args.interval,
            args.start_date,
            args.end_date,
            kline_db_batch_size,
        )
    except psycopg2.Error as db_err:
        print(f"Database error: {db_err}")
    except ValueError as ve:
        print(f"Date parsing or value error: {ve}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if db_connection:
            db_connection.close()
            print("Database connection closed.")
        print("Single symbol kline data fetching script finished.")
