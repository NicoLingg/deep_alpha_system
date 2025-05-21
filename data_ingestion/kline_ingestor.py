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
            open_time_utc = kline_row["time"]
            open_price = kline_row["open"]
            high_price = kline_row["high"]
            low_price = kline_row["low"]
            close_price = kline_row["close"]
            volume = kline_row["volume"]
            quote_asset_volume = kline_row.get("quote_volume", Decimal("0.0"))

            if (
                not isinstance(open_time_utc, pd.Timestamp)
                or open_time_utc.tzinfo is None
            ):
                raise ValueError(
                    "Kline time must be a timezone-aware Pandas Timestamp (UTC)."
                )

            # Ensure all numeric values are Decimal
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
            quote_asset_volume = (
                Decimal(str(quote_asset_volume))
                if not isinstance(quote_asset_volume, Decimal)
                else quote_asset_volume
            )

            records_to_insert.append(
                (
                    open_time_utc,
                    symbol_id,
                    interval,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    quote_asset_volume,
                )
            )
        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            print(
                f"\nWarning: Skipping malformed kline data for symbol_id {symbol_id} from {exchange_name}: {kline_row}. Error: {e}"
            )
            continue

    if not records_to_insert:
        return 0

    query = """
        INSERT INTO klines (time, symbol_id, interval, open_price, high_price, low_price, close_price,
                            volume, quote_asset_volume)
        VALUES %s
        ON CONFLICT (time, symbol_id, interval) DO UPDATE SET
            open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            quote_asset_volume = EXCLUDED.quote_asset_volume,
            ingested_at = CURRENT_TIMESTAMP;
    """
    inserted_count_for_batch = 0
    with conn.cursor() as cursor:
        try:
            execute_values(
                cursor, query, records_to_insert, page_size=len(records_to_insert)
            )
            conn.commit()
            inserted_count_for_batch = (
                cursor.rowcount
            )  # execute_values doesn't directly return count of affected rows easily for ON CONFLICT
            # rowcount here tells how many rows were affected by the command.
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
    )  # psycopg2 might return -1


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
    kline_batch_size: int = 500,
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

    if not min_time_local:
        fetch_tasks.append(
            (req_start_dt, api_req_end_dt_param, "full range (no local data)")
        )
    else:
        if req_start_dt < min_time_local:
            backfill_end_dt = min_time_local - interval_delta
            if backfill_end_dt >= req_start_dt:
                fetch_tasks.append(
                    (req_start_dt, backfill_end_dt, "older data (backfill)")
                )

        start_for_newer_data_dt = max(max_time_local + interval_delta, req_start_dt)
        if api_req_end_dt_param:
            if api_req_end_dt_param >= start_for_newer_data_dt:
                fetch_tasks.append(
                    (
                        start_for_newer_data_dt,
                        api_req_end_dt_param,
                        "newer data (to fixed end)",
                    )
                )
        else:  # Fetching to latest
            if start_for_newer_data_dt <= pd.Timestamp.utcnow().tz_convert(
                None
            ).tz_localize(
                "UTC"
            ):  # Ensure comparable
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
            klines_df_this_task = await exchange_adapter.fetch_klines(
                standard_symbol_str=standard_symbol_str,
                interval=interval_str,
                start_datetime=effective_api_start_dt,
                end_datetime=effective_api_end_dt,
            )
            if not klines_df_this_task.empty:
                processed_count = store_klines_batch(
                    db_conn, klines_df_this_task, symbol_id, interval_str, exchange_name
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
            continue

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

        kline_db_batch_size = int(
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
            if (
                s_repr.instrument_type == "FUTURE" and s_repr.expiry_date
            ):  # Handle specific future type from SymbolRepresentation
                type_for_db = f"FUTURE_{s_repr.expiry_date}"

            if args.base_asset:
                base_for_db = args.base_asset.upper()
            if args.quote_asset:
                quote_for_db = args.quote_asset.upper()
            # Type for DB primarily from parsed symbol, but override if --instrument-type is more specific
            if (
                args.instrument_type and args.instrument_type.upper() != "SPOT"
            ):  # if --instrument-type is given and not SPOT, use it
                if (
                    args.instrument_type.upper().startswith("FUTURE")
                    and s_repr.expiry_date
                ):
                    # if future, ensure expiry from parsed symbol is used
                    type_for_db = f"FUTURE_{s_repr.expiry_date}"
                else:
                    type_for_db = args.instrument_type.upper()

            if not base_for_db or not quote_for_db:
                print(
                    f"Error: Could not determine standard base and quote assets from --symbol '{args.symbol}' or --base-asset/--quote-asset arguments."
                )
                exit(1)

            exchange_instrument_name_for_db = (
                exchange_adapter_instance.normalize_standard_symbol_to_exchange(
                    args.symbol
                )
            )

            with db_connection.cursor(cursor_factory=DictCursor) as cursor:
                exchange_id_val = get_or_create_exchange_id(cursor, args.exchange)
                db_connection.commit()

                symbol_id_val, _ = get_or_create_symbol_id(
                    cursor,
                    exchange_id_val,
                    exchange_instrument_name_for_db,
                    base_for_db,
                    quote_for_db,
                    type_for_db,
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
                args.symbol,
                exchange_instrument_name_for_db,
                args.interval,
                args.start_date,
                args.end_date,
                kline_db_batch_size,
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
