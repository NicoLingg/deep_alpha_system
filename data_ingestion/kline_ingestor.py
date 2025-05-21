import psycopg2
import argparse
import pandas as pd
from psycopg2.extras import execute_values, DictCursor
from datetime import datetime as dt, timezone, timedelta
from typing import Optional, List, Tuple, Any
from decimal import Decimal, InvalidOperation
import asyncio

from .utils import (
    load_config,
    get_db_connection,
    get_exchange_adapter,
    get_or_create_exchange_id,
    get_or_create_symbol_id,
    DEFAULT_CONFIG_PATH,
)
from .exchanges.base_interface import ExchangeInterface


def store_klines_batch(
    conn,
    klines_data_df: pd.DataFrame,
    symbol_id: int,
    interval: str,
    exchange_name: str,
) -> int:
    if klines_data_df.empty:
        return 0

    records_to_insert: List[Tuple[Any, ...]] = []
    for _, kline_row in klines_data_df.iterrows():
        try:
            open_time_utc = kline_row["time"]  # This is a pd.Timestamp from adapter
            open_price = kline_row["open"]
            high_price = kline_row["high"]
            low_price = kline_row["low"]
            close_price = kline_row["close"]
            volume = kline_row["volume"]

            # Standardized column name from adapter is 'quote_volume'
            quote_asset_volume_val = kline_row.get("quote_volume")
            if quote_asset_volume_val is None or pd.isna(quote_asset_volume_val):
                quote_asset_volume_val = Decimal("0.0")  # Default if not provided or NA
            else:
                quote_asset_volume_val = (
                    Decimal(str(quote_asset_volume_val))
                    if not isinstance(quote_asset_volume_val, Decimal)
                    else quote_asset_volume_val
                )

            # Standardized column name from adapter is 'close_timestamp'
            close_timestamp_val = kline_row.get("close_timestamp")
            if (
                pd.isna(close_timestamp_val) or close_timestamp_val is None
            ):  # Handles pd.NaT, None
                close_timestamp_val = None
            elif isinstance(
                close_timestamp_val, pd.Timestamp
            ):  # Convert to python datetime for psycopg2
                close_timestamp_val = close_timestamp_val.to_pydatetime()

            # Standardized column name from adapter is 'trade_count'
            trade_count_val = kline_row.get("trade_count")
            if (
                pd.isna(trade_count_val) or trade_count_val is None
            ):  # Handles pd.NA from Int64Dtype, None
                trade_count_val = None

            # Standardized column name from adapter is 'taker_base_volume'
            taker_base_volume_val = kline_row.get("taker_base_volume")
            if taker_base_volume_val is None or pd.isna(taker_base_volume_val):
                taker_base_volume_val = None  # Nullable in DB
            else:
                taker_base_volume_val = (
                    Decimal(str(taker_base_volume_val))
                    if not isinstance(taker_base_volume_val, Decimal)
                    else taker_base_volume_val
                )

            # Standardized column name from adapter is 'taker_quote_volume'
            taker_quote_volume_val = kline_row.get("taker_quote_volume")
            if taker_quote_volume_val is None or pd.isna(taker_quote_volume_val):
                taker_quote_volume_val = None  # Nullable in DB
            else:
                taker_quote_volume_val = (
                    Decimal(str(taker_quote_volume_val))
                    if not isinstance(taker_quote_volume_val, Decimal)
                    else taker_quote_volume_val
                )

            if (
                not isinstance(open_time_utc, pd.Timestamp)
                or open_time_utc.tzinfo is None
            ):
                raise ValueError(
                    "Kline time must be a timezone-aware Pandas Timestamp (UTC)."
                )

            # Ensure all numeric values are Decimal (already done by adapter for main ones, but good to ensure for DB)
            open_price = (
                Decimal(str(open_price))
                if not isinstance(open_price, Decimal)
                else open_price
            )
            high_price = (
                Decimal(str(high_price))
                if not isinstance(high_price, Decimal)
                else high_price
            )
            low_price = (
                Decimal(str(low_price))
                if not isinstance(low_price, Decimal)
                else low_price
            )
            close_price = (
                Decimal(str(close_price))
                if not isinstance(close_price, Decimal)
                else close_price
            )
            volume = Decimal(str(volume)) if not isinstance(volume, Decimal) else volume
            # quote_asset_volume_val is already handled

            records_to_insert.append(
                (
                    open_time_utc.to_pydatetime(),  # Convert pd.Timestamp to python datetime
                    symbol_id,
                    interval,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    quote_asset_volume_val,  # Corresponds to DB 'quote_asset_volume'
                    close_timestamp_val,  # Corresponds to DB 'close_time'
                    trade_count_val,  # Corresponds to DB 'number_of_trades'
                    taker_base_volume_val,  # Corresponds to DB 'taker_buy_base_asset_volume'
                    taker_quote_volume_val,  # Corresponds to DB 'taker_buy_quote_asset_volume'
                )
            )
        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            print(
                f"\nWarning: Skipping malformed kline data for symbol_id {symbol_id} from {exchange_name}: {kline_row}. Error: {e}"
            )
            import traceback  # Temp for debugging

            traceback.print_exc()  # Temp for debugging
            continue

    if not records_to_insert:
        return 0

    query = """
        INSERT INTO klines (time, symbol_id, interval, open_price, high_price, low_price, close_price,
                            volume, quote_asset_volume,
                            close_time, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
        VALUES %s
        ON CONFLICT (time, symbol_id, interval) DO UPDATE SET
            open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            quote_asset_volume = EXCLUDED.quote_asset_volume,
            close_time = EXCLUDED.close_time,
            number_of_trades = EXCLUDED.number_of_trades,
            taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
            ingested_at = CURRENT_TIMESTAMP;
    """
    inserted_count_for_batch = 0
    with conn.cursor() as cursor:
        try:
            execute_values(
                cursor, query, records_to_insert, page_size=len(records_to_insert)
            )
            conn.commit()
            inserted_count_for_batch = cursor.rowcount
        except psycopg2.Error as e:
            conn.rollback()
            print(
                f"\nDB error inserting klines for symbol_id {symbol_id} from {exchange_name}: {e}"
            )
            return 0
    return (
        inserted_count_for_batch
        if inserted_count_for_batch >= 0
        else len(records_to_insert)
    )


def get_local_kline_daterange(
    db_conn, symbol_id: int, interval: str
) -> Tuple[Optional[dt], Optional[dt]]:
    min_time_local, max_time_local = None, None
    try:
        with db_conn.cursor(cursor_factory=DictCursor) as cursor:
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
    try:
        num_part = int(interval_str[:-1])
        unit_part = interval_str[-1].lower()
        if unit_part == "m":
            return timedelta(minutes=num_part)
        elif unit_part == "h":
            return timedelta(hours=num_part)
        elif unit_part == "d":
            return timedelta(days=num_part)
        elif unit_part == "w":
            return timedelta(weeks=num_part)
    except ValueError:
        pass
    raise ValueError(f"Unsupported interval string for timedelta: {interval_str}")


async def fetch_and_store_historical_klines(
    exchange_adapter: ExchangeInterface,
    db_conn,
    symbol_id: int,
    standard_symbol_str: str,
    exchange_instrument_name: str,  # For logging/display purposes
    interval_str: str,
    requested_start_str_dt: str,
    requested_end_str_dt: Optional[str] = None,
    kline_batch_size: int = 500,  # This parameter is not directly used by fetch_klines, but for chunking if API had limits smaller than full range
) -> int:
    total_processed_for_symbol_run = 0
    exchange_name = exchange_adapter.get_exchange_name()

    try:
        req_start_dt = pd.to_datetime(requested_start_str_dt, errors="raise", utc=True)
    except Exception as e:
        print(
            f"\n[{exchange_instrument_name}@{exchange_name}] Invalid start_date format: '{requested_start_str_dt}'. Must be UTC datetime string. Error: {e}"
        )
        return 0

    req_end_dt: Optional[pd.Timestamp] = None
    if requested_end_str_dt:
        try:
            req_end_dt = pd.to_datetime(requested_end_str_dt, errors="raise", utc=True)
            if req_end_dt < req_start_dt:
                print(
                    f"\n[{exchange_instrument_name}@{exchange_name}] Requested end_date '{requested_end_str_dt}' is before start_date '{requested_start_str_dt}'. Skipping."
                )
                return 0
        except Exception as e:
            print(
                f"\n[{exchange_instrument_name}@{exchange_name}] Invalid end_date format: '{requested_end_str_dt}'. Error: {e}"
            )
            return 0

    print(
        f"\n[{exchange_instrument_name}@{exchange_name}] Request: Fetch from '{req_start_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}' to '{req_end_dt.strftime('%Y-%m-%d %H:%M:%S %Z') if req_end_dt else 'latest'}' for interval '{interval_str}'."
    )

    min_time_local, max_time_local = get_local_kline_daterange(
        db_conn, symbol_id, interval_str
    )
    try:
        interval_delta = get_interval_timedelta(interval_str)
    except ValueError as e:
        print(f"\n[{exchange_instrument_name}@{exchange_name}] {e}. Skipping symbol.")
        return 0

    if min_time_local:
        print(
            f"[{exchange_instrument_name}@{exchange_name}] Local data found: {min_time_local.strftime('%Y-%m-%d %H:%M:%S %Z')} to {max_time_local.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )
    else:
        print(
            f"[{exchange_instrument_name}@{exchange_name}] No local data found for this symbol/interval."
        )

    fetch_tasks: List[Tuple[pd.Timestamp, Optional[pd.Timestamp], str]] = []
    api_req_end_dt_param = req_end_dt

    # Logic to determine fetch ranges based on local data and requested range
    # This part assumes the adapter's fetch_klines can handle a large date range
    # or that it internally pages. If not, this fetch_and_store_historical_klines
    # would need to loop and make smaller calls to the adapter.
    # For now, we'll make one call per "gap" identified.

    current_fetch_start_dt = req_start_dt

    # Task 1: Backfill older data if needed
    if not min_time_local or req_start_dt < min_time_local:
        effective_end_for_backfill = api_req_end_dt_param
        if min_time_local and (
            api_req_end_dt_param is None or api_req_end_dt_param >= min_time_local
        ):
            # If fetching to latest or beyond current local data, only backfill up to local start
            effective_end_for_backfill = min_time_local - interval_delta

        if (
            effective_end_for_backfill is None
            or effective_end_for_backfill >= req_start_dt
        ):
            fetch_tasks.append(
                (
                    req_start_dt,
                    effective_end_for_backfill,
                    (
                        "older data (backfill)"
                        if min_time_local
                        else "full range (no local data)"
                    ),
                )
            )
            if effective_end_for_backfill is not None:
                current_fetch_start_dt = max(
                    current_fetch_start_dt, effective_end_for_backfill + interval_delta
                )

    # Task 2: Fetch newer data if needed
    if max_time_local:
        start_for_newer_data_dt = max(
            max_time_local + interval_delta, current_fetch_start_dt
        )
    else:  # No local data yet, means current_fetch_start_dt is req_start_dt
        start_for_newer_data_dt = current_fetch_start_dt
        if (
            fetch_tasks
        ):  # if full range was already added, this might be redundant or needs adjustment
            # if full range (req_start_dt to api_req_end_dt_param) was added, this logic is tricky
            # This logic is simplified, assuming one main call or distinct backfill/forwardfill blocks
            pass

    # The fetch_tasks logic needs to be robust against overlaps.
    # A simpler approach if adapter handles large ranges:
    # 1. Fetch missing data before min_time_local
    # 2. Fetch missing data after max_time_local up to req_end_dt (or now)

    # Simplified fetch_tasks logic (revisiting the original more robust one)
    fetch_tasks = []  # Resetting for clarity with original logic structure
    if not min_time_local:  # No local data at all
        fetch_tasks.append(
            (req_start_dt, api_req_end_dt_param, "full range (no local data)")
        )
    else:  # Local data exists
        # Fetch older data (backfill)
        if req_start_dt < min_time_local:
            backfill_end_dt = min_time_local - interval_delta
            if backfill_end_dt >= req_start_dt:  # Ensure end is not before start
                fetch_tasks.append(
                    (req_start_dt, backfill_end_dt, "older data (backfill)")
                )

        # Fetch newer data (forward-fill)
        # Start fetching from one interval after the latest local data, or from request_start_date if it's later
        start_for_newer_data_dt = max(max_time_local + interval_delta, req_start_dt)

        # Determine the end for this newer data fetch task
        end_for_newer_data_dt = api_req_end_dt_param  # Could be None (fetch to latest)

        if end_for_newer_data_dt:  # Fixed end date requested
            if end_for_newer_data_dt >= start_for_newer_data_dt:
                fetch_tasks.append(
                    (
                        start_for_newer_data_dt,
                        end_for_newer_data_dt,
                        "newer data (to fixed end)",
                    )
                )
        else:  # Fetching to latest
            # Only add task if start_for_newer_data is not in the future
            # Ensure timestamps are comparable (both timezone-aware or both naive)
            now_utc = pd.Timestamp.utcnow()  # This is already UTC
            if start_for_newer_data_dt <= now_utc:
                fetch_tasks.append(
                    (start_for_newer_data_dt, None, "newer data (to latest)")
                )

    if not fetch_tasks:
        print(
            f"[{exchange_instrument_name}@{exchange_name}] All data for the requested range seems to be locally available or request is for future data. No API calls needed now."
        )
        return 0

    for task_idx, (
        effective_api_start_dt,
        effective_api_end_dt,
        task_desc,
    ) in enumerate(fetch_tasks):
        start_log = effective_api_start_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        end_log = (
            effective_api_end_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
            if effective_api_end_dt
            else "latest"
        )
        print(
            f"[{exchange_instrument_name}@{exchange_name}] Task {task_idx+1}/{len(fetch_tasks)} ({task_desc}): API From '{start_log}' to '{end_log}'"
        )

        klines_df_this_task = pd.DataFrame()
        try:
            # The adapter's fetch_klines should handle pagination internally if necessary,
            # or this loop would need to manage it with the `limit` parameter.
            # Binance adapter's `get_historical_klines` has a limit (default 1000),
            # but it can be called for a specific date range. If the range implies more than `limit` klines,
            # it returns up to `limit` from the start of the range.
            # For true historical backfill over long periods, this function would need to loop,
            # advancing `effective_api_start_dt` until `effective_api_end_dt` is reached.
            # The current structure makes one call per identified "gap".

            # Assuming adapter's fetch_klines might return more than default limit if a large range is given
            # and it handles its own paging if needed. Or, it returns up to 'limit' (e.g., 1000 for Binance).
            # If it returns only up to adapter's internal limit (e.g. 1000 candles),
            # then this fetch_and_store logic needs to loop.
            # For now, assuming the adapter tries to get all klines in the date range specified.

            klines_df_this_task = await exchange_adapter.fetch_klines(
                standard_symbol_str=standard_symbol_str,
                interval=interval_str,
                start_datetime=effective_api_start_dt,
                end_datetime=effective_api_end_dt,
                limit=None,  # Let adapter use its default or handle large range (Binance default is 1000)
                # If we want to control chunks, pass kline_batch_size, but adapter needs to use it for paging
            )

            if not klines_df_this_task.empty:
                # The store_klines_batch will insert these. If there are many, it's one large DB transaction.
                # For extremely large results from fetch_klines (e.g. years of 1m data),
                # might be better to chunk klines_df_this_task before passing to store_klines_batch.

                # Simple chunking if dataframe is too large:
                if (
                    len(klines_df_this_task) > kline_batch_size * 1.5
                ):  # Heuristic to chunk if significantly larger
                    num_chunks = (
                        len(klines_df_this_task) + kline_batch_size - 1
                    ) // kline_batch_size
                    for i in range(num_chunks):
                        chunk_df = klines_df_this_task.iloc[
                            i * kline_batch_size : (i + 1) * kline_batch_size
                        ]
                        if not chunk_df.empty:
                            processed_count_chunk = store_klines_batch(
                                db_conn,
                                chunk_df,
                                symbol_id,
                                interval_str,
                                exchange_name,
                            )
                            total_processed_for_symbol_run += processed_count_chunk
                            print(
                                f"[{exchange_instrument_name}@{exchange_name}] Task {task_idx+1} ({task_desc}) - Chunk {i+1}/{num_chunks}: API returned {len(chunk_df)} (part of {len(klines_df_this_task)}), DB Processed/Updated: {processed_count_chunk}."
                            )
                            if processed_count_chunk > 0:
                                await asyncio.sleep(0.1)  # Small delay after DB write
                else:  # Process as a single batch
                    processed_count = store_klines_batch(
                        db_conn,
                        klines_df_this_task,
                        symbol_id,
                        interval_str,
                        exchange_name,
                    )
                    total_processed_for_symbol_run += processed_count
                    print(
                        f"[{exchange_instrument_name}@{exchange_name}] Task {task_idx+1} ({task_desc}): API returned {len(klines_df_this_task)} klines, DB Processed/Updated: {processed_count}."
                    )
                    if processed_count > 0:
                        await asyncio.sleep(0.1)
            else:
                print(
                    f"[{exchange_instrument_name}@{exchange_name}] Task {task_idx+1} ({task_desc}): No klines returned from API."
                )
        except Exception as e:
            print(
                f"\nError during {exchange_name} API call or DB store (Task {task_idx+1}, {task_desc}) for {exchange_instrument_name}: {e}"
            )
            import traceback

            traceback.print_exc()
            continue  # Continue to next task if one fails

    if total_processed_for_symbol_run > 0:
        print(
            f"[{exchange_instrument_name}@{exchange_name}] Completed. Total DB klines processed/updated: {total_processed_for_symbol_run}."
        )
    else:
        print(
            f"[{exchange_instrument_name}@{exchange_name}] Completed. No new klines inserted/updated."
        )
    return total_processed_for_symbol_run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download klines using exchange adapters."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config file (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--exchange", type=str, default="binance", help="Exchange name (e.g., binance)."
    )
    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Standard trading symbol (e.g., BTC-USDT or BTC-USD-PERP).",
    )
    parser.add_argument(
        "--base-asset",
        type=str,
        help="Standard base asset (e.g., BTC). Optional if symbol provides it.",
    )
    parser.add_argument(
        "--quote-asset",
        type=str,
        help="Standard quote asset (e.g., USDT or USD). Optional if symbol provides it.",
    )
    parser.add_argument(
        "--instrument-type",
        type=str,
        default="SPOT",
        help="Instrument type (SPOT, PERP, FUTURE). Optional if symbol provides it (e.g. BTC-USD-PERP).",
    )
    parser.add_argument(
        "--interval", type=str, default="1m", help="Kline interval (e.g., 1m, 5m, 1h)."
    )
    parser.add_argument(
        "--start-date", type=str, required=True, help="Kline start date/datetime (UTC)."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Kline end date/datetime (UTC). Default: latest.",
    )
    args = parser.parse_args()

    async def main_cli():
        config_object = None
        db_connection = None
        exchange_adapter_instance: Optional[ExchangeInterface] = None

        try:
            config_object = load_config(args.config)
        except FileNotFoundError as e:
            print(f"Configuration Error: {e}")
            exit(1)
        except KeyError as e:
            print(f"Configuration error: Missing key {e}")
            exit(1)

        kline_db_batch_size_cfg = int(  # Renamed to avoid conflict with function param
            config_object.get("settings", "kline_fetch_batch_size", fallback="500")
        )

        try:
            db_connection = get_db_connection(config_object)
            if db_connection is None:
                print("Failed to connect to the database. Exiting.")
                exit(1)

            exchange_adapter_instance = get_exchange_adapter(
                args.exchange, config_object
            )
            # Ensure exchange adapter's internal cache is populated for symbol normalization
            if hasattr(exchange_adapter_instance, "_ensure_cache_populated"):
                await exchange_adapter_instance._ensure_cache_populated()

            print(
                f"Successfully connected to DB and initialized {args.exchange.capitalize()} adapter."
            )

            from .exchanges.symbol_representation import (
                SymbolRepresentation,
            )  # Local import for script context

            s_repr = SymbolRepresentation.parse(args.symbol)
            base_for_db = s_repr.base_asset
            quote_for_db = s_repr.quote_asset
            type_for_db = s_repr.instrument_type
            # For DB storage, future type might be stored as FUTURE_YYMMDD or just FUTURE
            # get_or_create_symbol_id expects FUTURE_YYMMDD if expiry is present
            if s_repr.instrument_type == "FUTURE" and s_repr.expiry_date:
                type_for_db = f"FUTURE_{s_repr.expiry_date}"

            if args.base_asset:
                base_for_db = args.base_asset.upper()
            if args.quote_asset:
                quote_for_db = args.quote_asset.upper()

            # Instrument type override from args
            if args.instrument_type:
                arg_instrument_type_upper = args.instrument_type.upper()
                if (
                    arg_instrument_type_upper.startswith("FUTURE")
                    and s_repr.expiry_date
                ):
                    # If arg is FUTURE and symbol had expiry, ensure it's FUTURE_YYMMDD
                    type_for_db = f"FUTURE_{s_repr.expiry_date}"
                elif arg_instrument_type_upper == "PERP":
                    type_for_db = "PERP"
                elif arg_instrument_type_upper == "SPOT":
                    type_for_db = "SPOT"
                else:  # Generic type from arg if not matching specific structures
                    type_for_db = arg_instrument_type_upper

            if not base_for_db or not quote_for_db:
                print(
                    f"Error: Could not determine standard base and quote assets from --symbol '{args.symbol}' or --base-asset/--quote-asset arguments."
                )
                exit(1)

            exchange_instrument_name_for_db = (
                exchange_adapter_instance.normalize_standard_symbol_to_exchange(
                    args.symbol  # Pass the standard symbol string here
                )
            )

            with db_connection.cursor(cursor_factory=DictCursor) as cursor:
                exchange_id_val = get_or_create_exchange_id(cursor, args.exchange)
                db_connection.commit()

                symbol_id_val, _ = get_or_create_symbol_id(
                    cursor,
                    exchange_id_val,
                    exchange_instrument_name_for_db,  # This is exchange specific name
                    base_for_db,  # Standard base
                    quote_for_db,  # Standard quote
                    type_for_db,  # Standard type (e.g. SPOT, PERP, FUTURE_YYMMDD)
                )
                db_connection.commit()

            if symbol_id_val is None:
                print(
                    f"Could not process symbol {args.symbol} (std: {base_for_db}-{quote_for_db}-{type_for_db}, exch: {exchange_instrument_name_for_db}) in DB for exchange {args.exchange}. Exiting."
                )
                exit(1)

            print(
                f"Targeting Standard Symbol: {args.symbol} (Exchange: {args.exchange}, Instrument: {exchange_instrument_name_for_db}, DB ID: {symbol_id_val})"
            )

            await fetch_and_store_historical_klines(
                exchange_adapter_instance,
                db_connection,
                symbol_id_val,
                args.symbol,  # Pass the standard symbol string for adapter
                exchange_instrument_name_for_db,  # For logging
                args.interval,
                args.start_date,
                args.end_date,
                kline_db_batch_size_cfg,  # Pass the batch size from config
            )
        except psycopg2.Error as db_err:
            print(f"Database error: {db_err}")
        except ValueError as ve:
            print(f"Value error: {ve}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            import traceback

            traceback.print_exc()
        finally:
            if db_connection:
                db_connection.close()
                print("Database connection closed.")
            if exchange_adapter_instance and hasattr(
                exchange_adapter_instance, "close_session"
            ):
                await exchange_adapter_instance.close_session()
                print(f"{args.exchange.capitalize()} adapter session closed.")
            print("Kline data fetching script finished.")

    asyncio.run(main_cli())
