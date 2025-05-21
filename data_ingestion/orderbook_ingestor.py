import time
import json
import signal
import psycopg2
import pandas as pd
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
    try:
        sig_name = signal.Signals(signum).name
    except AttributeError:
        sig_name = f"Signal {signum}"
    logger.info(
        f"{sig_name} received, initiating graceful shutdown for order book collector..."
    )
    shutdown_flag = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def store_order_book_snapshot(
    db_conn, symbol_id: int, exchange_instrument_name: str, depth_data: Dict[str, Any]
) -> bool:
    current_ingestion_time_utc = datetime.now(timezone.utc)

    # lastUpdateId is critical and part of PK, must be present.
    # Check should happen in main_polling_loop before calling this.
    exchange_specific_luid = depth_data.get("lastUpdateId")
    if exchange_specific_luid is None:
        logger.error(
            f"store_order_book_snapshot called with lastUpdateId=None for {exchange_instrument_name}. This should be pre-checked. Skipping."
        )
        return False

    bids_for_json = []
    for p_raw, s_raw in depth_data.get("bids", []):
        try:
            p = Decimal(str(p_raw))
            s = Decimal(str(s_raw))
            bids_for_json.append([str(p), str(s)])
        except InvalidOperation:
            logger.warning(
                f"Invalid decimal value in bids for {exchange_instrument_name} (LUID: {exchange_specific_luid}): p='{p_raw}', s='{s_raw}'"
            )
            continue

    asks_for_json = []
    for p_raw, s_raw in depth_data.get("asks", []):
        try:
            p = Decimal(str(p_raw))
            s = Decimal(str(s_raw))
            asks_for_json.append([str(p), str(s)])
        except InvalidOperation:
            logger.warning(
                f"Invalid decimal value in asks for {exchange_instrument_name} (LUID: {exchange_specific_luid}): p='{p_raw}', s='{s_raw}'"
            )
            continue

    bids_json = json.dumps(bids_for_json)
    asks_json = json.dumps(asks_for_json)

    # 'exchange_ts' (pd.Timestamp or None) comes from adapter.
    # This is the exchange's event/transaction time if available (e.g., for Futures).
    # For Spot, it will be None.
    retrieved_at_from_exchange_payload = depth_data.get("exchange_ts")

    retrieved_at_for_db: datetime
    if retrieved_at_from_exchange_payload is not None:
        if isinstance(retrieved_at_from_exchange_payload, pd.Timestamp):
            retrieved_at_for_db = retrieved_at_from_exchange_payload.to_pydatetime()
        elif isinstance(retrieved_at_from_exchange_payload, datetime):
            retrieved_at_for_db = retrieved_at_from_exchange_payload
        else:
            logger.error(
                f"Unexpected type for exchange_ts: {type(retrieved_at_from_exchange_payload)} for {exchange_instrument_name}. Using current ingestion time for DB 'retrieved_at'."
            )
            retrieved_at_for_db = current_ingestion_time_utc
    else:
        logger.debug(
            f"Exchange timestamp (exchange_ts) is None for orderbook of {exchange_instrument_name}. Using current ingestion time for DB 'retrieved_at'."
        )
        retrieved_at_for_db = current_ingestion_time_utc

    if retrieved_at_for_db.tzinfo is None:
        retrieved_at_for_db = retrieved_at_for_db.replace(tzinfo=timezone.utc)
    else:
        retrieved_at_for_db = retrieved_at_for_db.astimezone(timezone.utc)

    luid_str = str(exchange_specific_luid)

    try:
        with db_conn.cursor() as cursor:
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
                    retrieved_at_for_db,
                    symbol_id,
                    luid_str,
                    bids_json,
                    asks_json,
                    current_ingestion_time_utc,
                ),
            )
            db_conn.commit()
            if cursor.rowcount > 0:
                logger.debug(
                    f"Stored order book for {exchange_instrument_name} (LUID: {luid_str}, RetrievedDB: {retrieved_at_for_db.strftime('%Y-%m-%d %H:%M:%S.%f %Z')})."
                )
            return True
    except psycopg2.Error as e:
        db_conn.rollback()
        logger.error(
            f"Database error storing order book for {exchange_instrument_name} (LUID: {luid_str}): {e}",
            exc_info=True,
        )
        return False
    except Exception as e:
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

            if (
                current_depth_data
                and current_depth_data.get("bids") is not None
                and current_depth_data.get("asks") is not None
                and current_depth_data.get("lastUpdateId") is not None
            ):

                if store_order_book_snapshot(
                    db_conn, symbol_id, exchange_instrument_name, current_depth_data
                ):
                    successful_stores += 1
            else:
                failed_fetches += 1
                missing_keys_info = ""
                if current_depth_data:
                    missing = []
                    if current_depth_data.get("bids") is None:
                        missing.append("bids")
                    if current_depth_data.get("asks") is None:
                        missing.append("asks")
                    if current_depth_data.get("lastUpdateId") is None:
                        missing.append("lastUpdateId")
                    if missing:
                        missing_keys_info = f" Missing keys: {', '.join(missing)}."

                logger.warning(
                    f"{log_prefix} Failed to fetch valid/complete order book.{missing_keys_info} "
                    f"Data (first 200 chars): {str(current_depth_data)[:200]}... Total fails: {failed_fetches}"
                )
        except Exception as e:
            failed_fetches += 1
            logger.error(
                f"{log_prefix} Error during fetch/store cycle: {e}", exc_info=True
            )
            if "rate limit" in str(e).lower() or (
                hasattr(e, "code") and getattr(e, "code") == -1003
            ):
                logger.warning(
                    f"{log_prefix} Rate limit likely hit, sleeping for 60 seconds..."
                )
                try:
                    await asyncio.sleep(60)
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
                logger.info(f"{log_prefix} Main sleep cancelled, shutting down.")
                break

        if (
            successful_stores > 0
            and successful_stores % 100 == 0
            and loop_start_time - (getattr(main_polling_loop, "last_log_time", 0)) > 300
        ):  # Log progress every ~5 mins if active
            logger.info(
                f"{log_prefix} Successfully stored {successful_stores} snapshots so far."
            )
            main_polling_loop.last_log_time = loop_start_time

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
        help="Standard trading symbol (e.g., BTC-USDT or BTC-USD-PERP). Will be stripped of whitespace.",
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

    # Strip whitespace from symbol argument
    if args.symbol:
        args.symbol = args.symbol.strip()

    async def main_cli():
        config_object = None
        db_connection = None
        exchange_adapter_instance: Optional[ExchangeInterface] = None

        try:
            config_object = load_config(args.config)
        except FileNotFoundError as e:
            logger.error(f"Configuration error: {e}")
            exit(1)
        except (
            KeyError,
            configparser.NoSectionError,
            configparser.NoOptionError,
        ) as e:  # More specific config errors
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
                await exchange_adapter_instance._ensure_cache_populated()  # Crucial for normalize_standard_symbol_to_exchange
            logger.info(
                f"Successfully connected to DB and initialized {args.exchange.capitalize()} adapter."
            )

            try:
                s_repr = SymbolRepresentation.parse(args.symbol)
            except ValueError as e:
                logger.error(f"Invalid --symbol argument '{args.symbol}': {e}")
                exit(1)

            base_for_db = (
                args.base_asset.strip().upper()
                if args.base_asset
                else s_repr.base_asset
            )
            quote_for_db = (
                args.quote_asset.strip().upper()
                if args.quote_asset
                else s_repr.quote_asset
            )

            # Determine DB instrument type
            type_for_db = s_repr.instrument_type  # Base type from parsed symbol
            if s_repr.instrument_type == FUTURE and s_repr.expiry_date:
                type_for_db = f"{FUTURE}_{s_repr.expiry_date}"

            if args.instrument_type:  # Override with CLI arg if provided
                arg_type_cleaned = args.instrument_type.strip().upper()
                if (
                    arg_type_cleaned.startswith(f"{FUTURE}_")
                    and len(arg_type_cleaned.split("_")) == 2
                    and arg_type_cleaned.split("_")[1].isdigit()
                ):
                    type_for_db = arg_type_cleaned
                elif arg_type_cleaned == FUTURE and s_repr.expiry_date:
                    type_for_db = f"{FUTURE}_{s_repr.expiry_date}"
                elif arg_type_cleaned in [
                    SPOT,
                    PERP,
                    FUTURE,
                ]:  # Allow generic FUTURE if no date
                    type_for_db = arg_type_cleaned
                else:
                    logger.warning(
                        f"Using custom instrument type '{arg_type_cleaned}' from --instrument-type."
                    )
                    type_for_db = arg_type_cleaned

            if not base_for_db or not quote_for_db:
                logger.error(
                    f"Could not determine standard base and quote assets from --symbol '{args.symbol}' or other arguments."
                )
                exit(1)

            standard_symbol_for_adapter = args.symbol  # Already stripped
            exchange_instrument_name_for_db = ""
            try:
                exchange_instrument_name_for_db = (
                    exchange_adapter_instance.normalize_standard_symbol_to_exchange(
                        standard_symbol_for_adapter
                    )
                )
            except ValueError as e:
                logger.error(
                    f"Failed to normalize standard symbol '{standard_symbol_for_adapter}' to exchange format for {args.exchange}: {e}"
                )
                exit(1)

            with db_connection.cursor(cursor_factory=DictCursor) as cursor:
                exchange_id_val = get_or_create_exchange_id(cursor, args.exchange)
                db_connection.commit()  # Commit exchange ID creation

                symbol_id_val, _ = get_or_create_symbol_id(
                    cursor,
                    exchange_id_val,
                    exchange_instrument_name_for_db,
                    base_for_db,
                    quote_for_db,
                    type_for_db,
                )
                db_connection.commit()  # Commit symbol ID creation

            if (
                symbol_id_val is None
            ):  # Should be caught by exception in get_or_create if it fails
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
        except (
            ValueError
        ) as ve:  # From load_config, get_exchange_adapter, SymbolRepresentation.parse etc.
            logger.error(f"Configuration or setup value error: {ve}", exc_info=True)
        except Exception as e:
            logger.error(
                f"An unexpected error occurred in main_cli: {e}", exc_info=True
            )
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
