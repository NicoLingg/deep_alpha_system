import time
import json
import signal
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, Any, List
import asyncio
import argparse

from .utils import (
    load_config,
    get_db_connection,
    get_exchange_adapter,
    get_or_create_exchange_id,
    get_or_create_symbol_id,
    DEFAULT_CONFIG_PATH,
)
from .exchanges.base_interface import ExchangeInterface
from .exchanges.symbol_representation import SymbolRepresentation


shutdown_flag = False


def signal_handler(signum, frame):
    global shutdown_flag
    print(
        f"Signal {signum} received, initiating graceful shutdown for order book collector..."
    )
    shutdown_flag = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def store_order_book_snapshot(
    db_conn, symbol_id: int, exchange_instrument_name: str, depth_data: Dict[str, Any]
) -> bool:
    snapshot_time_utc = datetime.now(timezone.utc)
    exchange_specific_luid = depth_data.get("lastUpdateId")

    bids_for_json = []
    for p_raw, s_raw in depth_data.get("bids", []):
        try:
            p = Decimal(str(p_raw))
            s = Decimal(str(s_raw))
            bids_for_json.append([str(p), str(s)])
        except InvalidOperation:
            print(
                f"Warning: Invalid decimal value in bids for {exchange_instrument_name}: p={p_raw}, s={s_raw}"
            )
            continue

    asks_for_json = []
    for p_raw, s_raw in depth_data.get("asks", []):
        try:
            p = Decimal(str(p_raw))
            s = Decimal(str(s_raw))
            asks_for_json.append([str(p), str(s)])
        except InvalidOperation:
            print(
                f"Warning: Invalid decimal value in asks for {exchange_instrument_name}: p={p_raw}, s={s_raw}"
            )
            continue

    bids_json = json.dumps(bids_for_json)
    asks_json = json.dumps(asks_for_json)

    retrieved_at_for_db = depth_data.get("exchange_ts")
    if retrieved_at_for_db is None:
        retrieved_at_for_db = snapshot_time_utc
    elif hasattr(retrieved_at_for_db, "to_pydatetime"):  # Handle pandas Timestamp
        retrieved_at_for_db = retrieved_at_for_db.to_pydatetime()

    if retrieved_at_for_db.tzinfo is None:
        retrieved_at_for_db = retrieved_at_for_db.replace(tzinfo=timezone.utc)
    else:
        retrieved_at_for_db = retrieved_at_for_db.astimezone(timezone.utc)

    try:
        with db_conn.cursor() as cursor:
            luid_str = (
                str(exchange_specific_luid)
                if exchange_specific_luid is not None
                else None
            )
            cursor.execute(
                """
                INSERT INTO order_book_snapshots (time, symbol_id, last_update_id, bids, asks, retrieved_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol_id, last_update_id) DO UPDATE SET
                    time = EXCLUDED.time,
                    bids = EXCLUDED.bids,
                    asks = EXCLUDED.asks,
                    retrieved_at = EXCLUDED.retrieved_at,
                    ingested_at = CURRENT_TIMESTAMP;
                """,
                (
                    snapshot_time_utc,
                    symbol_id,
                    luid_str,
                    bids_json,
                    asks_json,
                    retrieved_at_for_db,
                ),
            )
            db_conn.commit()
            if cursor.rowcount > 0:
                # print(f"[{snapshot_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}] Stored {exchange_instrument_name} order book (LUID: {luid_str}).")
                pass  # Reduce verbosity
            return True
    except psycopg2.Error as e:
        db_conn.rollback()
        print(f"Database error storing order book for {exchange_instrument_name}: {e}")
        return False
    except Exception as e:
        db_conn.rollback()
        print(
            f"Unexpected error storing order book for {exchange_instrument_name}: {e}"
        )
        import traceback

        traceback.print_exc()
        return False


async def main_polling_loop(
    exchange_adapter: ExchangeInterface,
    db_conn,
    symbol_id: int,
    standard_symbol_str: str,
    exchange_instrument_name: str,
    depth_limit: int,
    interval_seconds: int,
):
    global shutdown_flag
    exchange_name = exchange_adapter.get_exchange_name()
    print(
        f"Starting order book snapshot collection for {exchange_instrument_name} ({standard_symbol_str}) on {exchange_name} every {interval_seconds} seconds. Depth: {depth_limit}. Press Ctrl+C to stop."
    )

    successful_stores = 0
    failed_fetches = 0

    while not shutdown_flag:
        loop_start_time = time.monotonic()
        try:
            current_depth_data = await exchange_adapter.fetch_orderbook_snapshot(
                standard_symbol_str=standard_symbol_str, limit=depth_limit
            )
            if (
                current_depth_data
                and current_depth_data.get("bids") is not None
                and current_depth_data.get("asks") is not None
            ):
                if store_order_book_snapshot(
                    db_conn, symbol_id, exchange_instrument_name, current_depth_data
                ):
                    successful_stores += 1
            else:
                failed_fetches += 1
                print(
                    f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}] Failed to fetch valid order book for {exchange_instrument_name} from {exchange_name}. Data: {current_depth_data}. Total fails: {failed_fetches}"
                )
        except Exception as e:
            failed_fetches += 1
            current_time_str = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S %Z"
            )
            print(
                f"[{current_time_str}] Error during fetch/store for {exchange_instrument_name} on {exchange_name}: {e}"
            )
            import traceback

            traceback.print_exc()
            if "rate limit" in str(e).lower() or (
                hasattr(e, "code") and getattr(e, "code") == -1003
            ):
                print("Rate limit likely hit, sleeping for 60 seconds...")
                await asyncio.sleep(60)

        elapsed_time = time.monotonic() - loop_start_time
        sleep_duration = interval_seconds - elapsed_time
        if shutdown_flag:
            break
        if sleep_duration > 0:
            try:
                await asyncio.sleep(sleep_duration)
            except asyncio.CancelledError:
                print("Sleep cancelled, shutting down.")
                break

        if successful_stores > 0 and successful_stores % 100 == 0:
            print(
                f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}] Successfully stored {successful_stores} snapshots for {exchange_instrument_name}."
            )

    print(
        f"Order book collection loop stopped for {exchange_instrument_name}. Total stored: {successful_stores}, Fetch/Store errors: {failed_fetches}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Periodically collect order book snapshots using exchange adapters."
    )
    parser.add_argument(
        "--config", type=str, default=DEFAULT_CONFIG_PATH, help=f"Path to config file."
    )
    parser.add_argument(
        "--exchange", type=str, default="binance", help="Exchange name (e.g., binance)."
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="BTC-USDT",
        help="Standard trading symbol (e.g., BTC-USDT or BTC-USD-PERP).",
    )
    parser.add_argument(
        "--base-asset", type=str, help="Standard base asset (e.g., BTC). Optional."
    )  # Not directly used by loop, but for ID creation
    parser.add_argument(
        "--quote-asset",
        type=str,
        help="Standard quote asset (e.g., USDT or USD). Optional.",
    )  # Not directly used by loop
    parser.add_argument(
        "--instrument-type",
        type=str,
        default="SPOT",
        help="Instrument type (SPOT, PERP, FUTURE). Optional.",
    )  # Not directly used by loop
    parser.add_argument(
        "--limit", type=int, default=100, help="Number of depth levels."
    )
    parser.add_argument(
        "--interval", type=int, default=60, help="Collection interval in seconds."
    )
    args = parser.parse_args()

    async def main_cli():
        config_object = None
        db_connection = None
        exchange_adapter_instance: Optional[ExchangeInterface] = None

        try:
            config_object = load_config(args.config)
        except FileNotFoundError as e:
            print(f"Configuration error: {e}")
            exit(1)
        except KeyError as e:
            print(f"Configuration error: Missing key {e}")
            exit(1)

        try:
            db_connection = get_db_connection(config_object)
            if db_connection is None:
                print("Failed to connect to database. Exiting.")
                exit(1)

            exchange_adapter_instance = get_exchange_adapter(
                args.exchange, config_object
            )
            print(
                f"Successfully connected to DB and initialized {args.exchange.capitalize()} adapter."
            )

            s_repr = SymbolRepresentation.parse(args.symbol)
            base_for_db = (
                args.base_asset.upper() if args.base_asset else s_repr.base_asset
            )
            quote_for_db = (
                args.quote_asset.upper() if args.quote_asset else s_repr.quote_asset
            )
            type_for_db = (
                args.instrument_type.upper()
                if args.instrument_type
                else s_repr.instrument_type
            )

            if s_repr.instrument_type == "FUTURE" and s_repr.expiry_date:
                type_for_db = f"FUTURE_{s_repr.expiry_date}"

            if not base_for_db or not quote_for_db:
                print(
                    f"Error: Could not determine standard base and quote assets from --symbol '{args.symbol}' or --base-asset/--quote-asset arguments."
                )
                exit(1)

            standard_symbol_for_adapter = (
                args.symbol
            )  # Pass the user-provided standard symbol directly
            exchange_instrument_name_for_db = (
                exchange_adapter_instance.normalize_standard_symbol_to_exchange(
                    standard_symbol_for_adapter
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
                    f"Exiting: Symbol {args.symbol} (std: {standard_symbol_for_adapter}, exch: {exchange_instrument_name_for_db}) could not be processed in DB for exchange {args.exchange}."
                )
                exit(1)

            print(
                f"Targeting Standard Symbol: {standard_symbol_for_adapter} (Exchange: {args.exchange}, Instrument: {exchange_instrument_name_for_db}, DB ID: {symbol_id_val}) for order book snapshots."
            )

            await main_polling_loop(
                exchange_adapter_instance,
                db_connection,
                symbol_id_val,
                standard_symbol_for_adapter,
                exchange_instrument_name_for_db,
                args.limit,
                args.interval,
            )

        except psycopg2.Error as db_err:
            print(f"A database error occurred: {db_err}")
        except ValueError as ve:
            print(f"Configuration or setup error: {ve}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            import traceback

            traceback.print_exc()
        finally:
            if db_connection:
                db_connection.close()
                print("Database connection closed for order book collector.")
            if exchange_adapter_instance and hasattr(
                exchange_adapter_instance, "close_session"
            ):
                await exchange_adapter_instance.close_session()
                print(f"{args.exchange.capitalize()} adapter session closed.")
            print("Snapshot collector shut down.")

    asyncio.run(main_cli())
