# data_ingestion/orderbook_ingestor.py
import time
import json
import signal
import psycopg2
import pandas as pd  # For pd.Timestamp check
import configparser
from psycopg2.extras import DictCursor
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, Any, List
import asyncio
import argparse
import logging

from .utils import (
    load_config,
    get_db_connection,
    get_exchange_adapter,
    get_or_create_exchange_id,
    get_or_create_symbol_id,
    DEFAULT_CONFIG_PATH,
    setup_logging,
)
from .exchanges.base_interface import ExchangeInterface
from .exchanges.symbol_representation import SymbolRepresentation, FUTURE, SPOT, PERP


logger = logging.getLogger(__name__)
shutdown_flag = False


def signal_handler(signum, frame):
    global shutdown_flag
    logger.info(
        f"Signal {signal.Signals(signum).name} received, initiating graceful shutdown for order book collector..."
    )
    shutdown_flag = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def store_order_book_snapshot(
    db_conn, symbol_id: int, exchange_instrument_name: str, depth_data: Dict[str, Any]
) -> bool:
    current_ingestion_time_utc = datetime.now(
        timezone.utc
    )  # This will go into 'ingestion_time'
    exchange_specific_luid = depth_data.get("lastUpdateId")

    bids_for_json = []
    for p_raw, s_raw in depth_data.get("bids", []):
        try:
            # Ensure p_raw and s_raw are strings before Decimal conversion if they are not already
            p = Decimal(str(p_raw))
            s = Decimal(str(s_raw))
            bids_for_json.append([str(p), str(s)])  # Store as strings in JSON
        except InvalidOperation:
            logger.warning(
                f"Invalid decimal value in bids for {exchange_instrument_name}: p='{p_raw}', s='{s_raw}'"
            )
            continue

    asks_for_json = []
    for p_raw, s_raw in depth_data.get("asks", []):
        try:
            p = Decimal(str(p_raw))
            s = Decimal(str(s_raw))
            asks_for_json.append([str(p), str(s)])  # Store as strings in JSON
        except InvalidOperation:
            logger.warning(
                f"Invalid decimal value in asks for {exchange_instrument_name}: p='{p_raw}', s='{s_raw}'"
            )
            continue

    bids_json = json.dumps(bids_for_json)
    asks_json = json.dumps(asks_for_json)

    # retrieved_at is the timestamp from the exchange for when the snapshot was generated
    retrieved_at_from_exchange = depth_data.get(
        "exchange_ts"
    )  # This is a pd.Timestamp from adapter

    if retrieved_at_from_exchange is None:
        # This is critical for the partitioning key of order_book_snapshots table
        logger.error(
            f"Critical: Exchange timestamp (retrieved_at) is MISSING for orderbook snapshot "
            f"of {exchange_instrument_name}, LUID {exchange_specific_luid}. "
            f"This is required for partitioning. Skipping snapshot."
        )
        return False  # Must have retrieved_at for PK and partitioning

    if isinstance(retrieved_at_from_exchange, pd.Timestamp):
        retrieved_at_for_db = retrieved_at_from_exchange.to_pydatetime()
    elif isinstance(retrieved_at_from_exchange, datetime):
        retrieved_at_for_db = retrieved_at_from_exchange  # Already datetime
    else:
        logger.error(
            f"Critical: Exchange timestamp (retrieved_at) for {exchange_instrument_name} is of unexpected type: "
            f"{type(retrieved_at_from_exchange)}. Expected pd.Timestamp or datetime. Skipping."
        )
        return False

    # Ensure retrieved_at_for_db is timezone-aware (UTC)
    if retrieved_at_for_db.tzinfo is None:
        retrieved_at_for_db = retrieved_at_for_db.replace(tzinfo=timezone.utc)
    else:
        retrieved_at_for_db = retrieved_at_for_db.astimezone(timezone.utc)

    luid_str = (
        str(exchange_specific_luid) if exchange_specific_luid is not None else None
    )
    if luid_str is None:  # lastUpdateId is part of PK and should not be null
        logger.error(
            f"Critical: lastUpdateId is MISSING for orderbook snapshot of {exchange_instrument_name} at {retrieved_at_for_db}. Skipping."
        )
        return False

    try:
        with db_conn.cursor() as cursor:
            # Columns: retrieved_at, symbol_id, last_update_id, bids, asks, ingestion_time
            # PK: (retrieved_at, symbol_id, last_update_id)
            cursor.execute(
                """
                INSERT INTO order_book_snapshots (retrieved_at, symbol_id, last_update_id, bids, asks, ingestion_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (retrieved_at, symbol_id, last_update_id) DO UPDATE SET
                    bids = EXCLUDED.bids,
                    asks = EXCLUDED.asks,
                    ingestion_time = EXCLUDED.ingestion_time; 
                """,
                (
                    retrieved_at_for_db,  # For 'retrieved_at' column (PK, partitioning key)
                    symbol_id,  # Part of PK
                    luid_str,  # Part of PK
                    bids_json,
                    asks_json,
                    current_ingestion_time_utc,  # For 'ingestion_time' column
                ),
            )
            db_conn.commit()
            if cursor.rowcount > 0:
                logger.debug(
                    f"Stored order book for {exchange_instrument_name} (LUID: {luid_str}, Retrieved: {retrieved_at_for_db.strftime('%Y-%m-%d %H:%M:%S.%f %Z')})."
                )
            return True
    except psycopg2.Error as e:
        db_conn.rollback()
        logger.error(
            f"Database error storing order book for {exchange_instrument_name} (LUID: {luid_str}): {e}",
            exc_info=True,
        )
        return False
    except Exception as e:  # Catch other potential errors like JSON serialization
        db_conn.rollback()
        logger.error(
            f"Unexpected error storing order book for {exchange_instrument_name} (LUID: {luid_str}): {e}",
            exc_info=True,
        )
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
    log_prefix = f"[{exchange_instrument_name}@{exchange_name} ({standard_symbol_str})]"
    logger.info(
        f"{log_prefix} Starting order book snapshot collection every {interval_seconds}s. Depth: {depth_limit}. Press Ctrl+C to stop."
    )

    successful_stores = 0
    failed_fetches = 0

    while not shutdown_flag:
        loop_start_time = time.monotonic()
        try:
            current_depth_data = await exchange_adapter.fetch_orderbook_snapshot(
                standard_symbol_str=standard_symbol_str, limit=depth_limit
            )
            # Check for all necessary fields from adapter before attempting to store
            if (
                current_depth_data
                and current_depth_data.get("bids")
                is not None  # Bids can be empty list, but key should exist
                and current_depth_data.get("asks") is not None  # Asks can be empty list
                and current_depth_data.get("lastUpdateId") is not None
                and current_depth_data.get("exchange_ts") is not None
            ):  # exchange_ts is crucial now for PK

                if store_order_book_snapshot(
                    db_conn, symbol_id, exchange_instrument_name, current_depth_data
                ):
                    successful_stores += 1
            else:
                failed_fetches += 1
                logger.warning(
                    f"{log_prefix} Failed to fetch valid/complete order book. "
                    f"Data (first 200 chars): {str(current_depth_data)[:200]}... Total fails: {failed_fetches}"
                )
        except Exception as e:
            failed_fetches += 1
            logger.error(f"{log_prefix} Error during fetch/store: {e}", exc_info=True)
            if "rate limit" in str(e).lower() or (
                hasattr(e, "code") and getattr(e, "code") == -1003
            ):  # Example Binance rate limit code
                logger.warning(
                    f"{log_prefix} Rate limit likely hit, sleeping for 60 seconds..."
                )
                try:
                    await asyncio.sleep(60)  # Backoff
                except asyncio.CancelledError:
                    logger.info(
                        f"{log_prefix} Sleep (rate limit backoff) cancelled, shutting down."
                    )
                    break

        elapsed_time = time.monotonic() - loop_start_time
        sleep_duration = interval_seconds - elapsed_time

        if shutdown_flag:
            break
        if sleep_duration > 0:
            try:
                await asyncio.sleep(sleep_duration)
            except asyncio.CancelledError:
                logger.info(f"{log_prefix} Sleep cancelled, shutting down.")
                break

        if (
            successful_stores > 0
            and successful_stores % 100 == 0
            and successful_stores > (failed_fetches * 2)
        ):
            logger.info(
                f"{log_prefix} Successfully stored {successful_stores} snapshots."
            )

    logger.info(
        f"{log_prefix} Order book collection loop stopped. Total stored: {successful_stores}, Fetch/Store errors: {failed_fetches}"
    )


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Periodically collect order book snapshots."
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
        "--base-asset",
        type=str,
        help="Standard base asset. Optional, derived from --symbol.",
    )
    parser.add_argument(
        "--quote-asset",
        type=str,
        help="Standard quote asset. Optional, derived from --symbol.",
    )
    parser.add_argument(
        "--instrument-type",
        type=str,
        help=f"Instrument type ({SPOT}, {PERP}, {FUTURE}). Optional, derived from --symbol or defaults to SPOT.",
    )
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
            logger.error(f"Configuration error: {e}")
            exit(1)
        except (KeyError, configparser.NoSectionError, configparser.NoOptionError) as e:
            logger.error(f"Configuration error: {e}")
            exit(1)

        try:
            db_connection = get_db_connection(config_object)
            if db_connection is None:
                logger.error("Failed to connect to database. Exiting.")
                exit(1)

            exchange_adapter_instance = get_exchange_adapter(
                args.exchange, config_object
            )
            if hasattr(exchange_adapter_instance, "_ensure_cache_populated"):
                await exchange_adapter_instance._ensure_cache_populated()
            logger.info(
                f"Successfully connected to DB and initialized {args.exchange.capitalize()} adapter."
            )

            try:
                s_repr = SymbolRepresentation.parse(args.symbol)
            except ValueError as e:
                logger.error(f"Invalid --symbol argument '{args.symbol}': {e}")
                exit(1)

            base_for_db = (
                args.base_asset.upper() if args.base_asset else s_repr.base_asset
            )
            quote_for_db = (
                args.quote_asset.upper() if args.quote_asset else s_repr.quote_asset
            )
            type_for_db = s_repr.instrument_type
            if s_repr.instrument_type == FUTURE and s_repr.expiry_date:
                type_for_db = f"{FUTURE}_{s_repr.expiry_date}"

            if args.instrument_type:
                arg_type_upper = args.instrument_type.upper()
                if arg_type_upper.startswith(f"{FUTURE}_") or arg_type_upper in [
                    SPOT,
                    PERP,
                ]:
                    type_for_db = arg_type_upper
                elif arg_type_upper == FUTURE and s_repr.expiry_date:
                    type_for_db = f"{FUTURE}_{s_repr.expiry_date}"
                else:
                    type_for_db = arg_type_upper

            if not base_for_db or not quote_for_db:
                logger.error(
                    f"Could not determine standard base and quote assets from --symbol '{args.symbol}' or other arguments."
                )
                exit(1)

            standard_symbol_for_adapter = args.symbol
            try:
                exchange_instrument_name_for_db = (
                    exchange_adapter_instance.normalize_standard_symbol_to_exchange(
                        standard_symbol_for_adapter
                    )
                )
            except ValueError as e:
                logger.error(
                    f"Failed to normalize standard symbol '{args.symbol}' to exchange format: {e}. Ensure adapter cache is populated or symbol is valid."
                )
                exit(1)

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
                logger.error(
                    f"Exiting: Symbol {args.symbol} (Std: {standard_symbol_for_adapter}, Exch: {exchange_instrument_name_for_db}) could not be processed in DB for exchange {args.exchange}."
                )
                exit(1)

            logger.info(
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
            logger.error(f"A database error occurred: {db_err}", exc_info=True)
        except ValueError as ve:
            logger.error(f"Configuration or setup error: {ve}", exc_info=True)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        finally:
            if db_connection:
                db_connection.close()
                logger.info("Database connection closed for order book collector.")
            if exchange_adapter_instance and hasattr(
                exchange_adapter_instance, "close_session"
            ):
                await exchange_adapter_instance.close_session()
            logger.info("Snapshot collector shut down.")

    asyncio.run(main_cli())
