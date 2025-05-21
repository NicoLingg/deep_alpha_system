# data_ingestion/manage_symbols.py
import argparse
import psycopg2
from psycopg2.extras import DictCursor
from typing import Optional, Tuple, List, Dict, Any
import asyncio
import logging

from .utils import (
    load_config,
    get_db_connection,
    get_exchange_adapter,
    get_or_create_exchange_id,
    get_or_create_symbol_id,
    DEFAULT_CONFIG_PATH,
    setup_logging,  # Added
)
from .exchanges.base_interface import ExchangeInterface

logger = logging.getLogger(__name__)


async def update_symbol_definitions_from_exchange_api(
    exchange_adapter: ExchangeInterface,
    db_conn,
) -> Tuple[int, int, int, int]:
    exchange_name = exchange_adapter.get_exchange_name()
    logger.info(
        f"Stage 1: Updating symbol definitions in local DB from {exchange_name.capitalize()} API..."
    )

    try:
        # fetch_exchange_symbols_info already calls _ensure_cache_populated(force_refresh=True)
        all_api_symbols_info = await exchange_adapter.fetch_exchange_symbols_info()
    except Exception as e:
        logger.error(
            f"API Error fetching exchange info from {exchange_name}: {e}", exc_info=True
        )
        return 0, 0, 0, 0

    if not all_api_symbols_info:
        logger.warning(f"No symbols data found from {exchange_name.capitalize()} API.")
        return 0, 0, 0, 0

    logger.info(
        f"Received {len(all_api_symbols_info)} symbol entries from {exchange_name.capitalize()} API."
    )

    newly_added_count = 0
    already_exist_count = 0
    skipped_count = 0
    db_error_count = 0

    with db_conn.cursor(cursor_factory=DictCursor) as cursor:
        exchange_id_val = get_or_create_exchange_id(cursor, exchange_name)
        db_conn.commit()  # Commit exchange_id creation before loop

        for i, s_info in enumerate(all_api_symbols_info):
            exchange_instrument_name = s_info.get("exchange_specific_symbol")
            standard_base_asset = s_info.get("base_asset")
            standard_quote_asset = s_info.get("quote_asset")
            # instrument_type from adapter can be "SPOT", "PERP", "FUTURE", or "FUTURE_YYMMDD"
            standard_instrument_type = s_info.get("instrument_type", "SPOT")
            api_status = s_info.get(
                "status"
            )  # Currently not used for filtering here, but good to have

            if not all(
                [exchange_instrument_name, standard_base_asset, standard_quote_asset]
            ):
                logger.warning(f"Skipping symbol due to missing core info: {s_info}")
                skipped_count += 1
                continue

            try:
                # get_or_create_symbol_id expects instrument_type like SPOT, PERP, FUTURE_YYMMDD
                # The adapter's fetch_exchange_symbols_info should provide this in `instrument_type` key.
                symbol_id, created = get_or_create_symbol_id(
                    cursor,
                    exchange_id_val,
                    exchange_instrument_name,
                    standard_base_asset,
                    standard_quote_asset,
                    standard_instrument_type,  # This should be the fully specified type
                )
                db_conn.commit()  # Commit after each symbol to save progress

                if created:
                    newly_added_count += 1
                else:
                    already_exist_count += 1

            except psycopg2.Error as db_e:
                db_conn.rollback()
                logger.error(
                    f"DB Error processing symbol {exchange_instrument_name} ({standard_base_asset}-{standard_quote_asset}-{standard_instrument_type}) for {exchange_name}: {db_e}",
                    exc_info=False,  # Keep log cleaner for many errors
                )
                db_error_count += 1
            except Exception as e_upsert:
                db_conn.rollback()
                logger.error(
                    f"Unexpected Error during get_or_create for symbol {exchange_instrument_name} on {exchange_name}: {e_upsert}",
                    exc_info=True,
                )
                db_error_count += 1

            if (i + 1) % 200 == 0:
                logger.info(
                    f" ...processed {i+1}/{len(all_api_symbols_info)} API entries for {exchange_name} definitions."
                )

    logger.info(
        f"--- Symbol Definition Update Summary for {exchange_name.capitalize()} ---"
    )
    logger.info(f"Total symbols from API: {len(all_api_symbols_info)}")
    logger.info(f"Symbols skipped (missing info): {skipped_count}")
    logger.info(f"New symbols added to DB: {newly_added_count}")
    logger.info(f"Symbols already existing in DB: {already_exist_count}")
    logger.info(f"DB errors during processing: {db_error_count}")
    return newly_added_count, already_exist_count, skipped_count, db_error_count


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Updates local symbol definitions from an exchange API."
    )
    parser.add_argument(
        "--config", type=str, default=DEFAULT_CONFIG_PATH, help="Path to config file."
    )
    parser.add_argument(
        "--exchange", type=str, default="binance", help="Exchange name (e.g., binance)."
    )
    args = parser.parse_args()

    async def main_cli():
        config_obj = None
        db_connection = None
        exchange_adapter_instance: Optional[ExchangeInterface] = None

        logger.info(
            f"--- Running Symbol Management for {args.exchange.capitalize()} ---"
        )
        try:
            config_obj = load_config(args.config)
            exchange_adapter_instance = get_exchange_adapter(args.exchange, config_obj)
            # Adapter's fetch_exchange_symbols_info will handle its own cache population/refresh.

            db_connection = get_db_connection(config_obj)
            if db_connection is None:
                logger.error("Failed to connect to database. Exiting.")
                exit(1)

            await update_symbol_definitions_from_exchange_api(
                exchange_adapter_instance, db_connection
            )

        except ValueError as ve:  # Config or setup errors from utils
            logger.error(f"Configuration or setup error: {ve}", exc_info=True)
        except Exception as e:
            logger.error(
                f"Main execution error in manage_symbols for {args.exchange.capitalize()}: {e}",
                exc_info=True,
            )
        finally:
            if db_connection:
                db_connection.close()
                logger.info("Database connection closed.")
            if exchange_adapter_instance and hasattr(
                exchange_adapter_instance, "close_session"
            ):
                await exchange_adapter_instance.close_session()
            logger.info(
                f"Symbol definition update script for {args.exchange.capitalize()} finished."
            )

    asyncio.run(main_cli())
