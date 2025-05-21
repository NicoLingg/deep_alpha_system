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
    setup_logging,
)
from .exchanges.base_interface import ExchangeInterface
from .exchanges.symbol_representation import SymbolRepresentation, SPOT, PERP, FUTURE

logger = logging.getLogger(__name__)


async def update_symbol_definitions_from_exchange_api(
    exchange_adapter: ExchangeInterface,
    db_conn,
    quote_asset_filter: Optional[str] = None,
    api_status_filter: Optional[str] = None,
    instrument_type_filter: Optional[str] = None,
    symbols_list_filter: Optional[str] = None,
    dry_run: bool = False,
) -> Tuple[int, int, int, int]:
    exchange_name = exchange_adapter.get_exchange_name()
    logger.info(
        f"Stage 1: Updating symbol definitions in local DB from {exchange_name.capitalize()} API..."
    )

    try:
        all_api_symbols_info = await exchange_adapter.fetch_exchange_symbols_info()
    except Exception as e:
        logger.error(
            f"API Error fetching exchange info from {exchange_name}: {e}", exc_info=True
        )
        return 0, 0, 0, 0

    if not all_api_symbols_info:
        logger.warning(f"No symbols data found from {exchange_name.capitalize()} API.")
        return 0, 0, 0, 0

    original_api_count = len(all_api_symbols_info)
    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}Received {original_api_count} symbol entries from {exchange_name.capitalize()} API before filtering."
    )


    # --- Apply Filters ---
    filtered_symbols_from_api = all_api_symbols_info
    count_after_status_filter = original_api_count
    count_after_quote_filter = original_api_count
    count_after_instrument_type_filter = original_api_count
    count_after_symbols_list_filter = original_api_count

    # 1. API Status Filter
    if api_status_filter and api_status_filter.upper() not in ["ANY", "NONE"]:
        current_len = len(filtered_symbols_from_api)
        filter_type = "TRADING_ONLY" # Default interpretation
        custom_statuses: List[str] = []

        if api_status_filter.upper().startswith("CUSTOM:"):
            filter_type = "CUSTOM"
            custom_statuses_part = api_status_filter.split("CUSTOM:", 1)[1]
            custom_statuses = [s.strip().upper() for s in custom_statuses_part.split(",") if s.strip()]
        elif api_status_filter.upper() == "TRADING_ONLY":
            filter_type = "TRADING_ONLY"
        # "ANY" or "NONE" means no status filtering here, or handled by initial check

        temp_list = []
        for s_info in filtered_symbols_from_api:
            # Adapters should provide 'status'. Assume 'TRADING' is the desirable active status.
            # A more robust solution might involve adapters providing a normalized 'is_tradable_api' boolean.
            live_api_status = s_info.get("status", "UNKNOWN").upper()
            
            proceed = True
            if filter_type == "TRADING_ONLY":
                if live_api_status != "TRADING": # Simple check for common status
                    proceed = False
            elif filter_type == "CUSTOM":
                if live_api_status not in custom_statuses:
                    proceed = False
            
            if proceed:
                temp_list.append(s_info)
        filtered_symbols_from_api = temp_list
        count_after_status_filter = len(filtered_symbols_from_api)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Filtered by API status '{api_status_filter}': {count_after_status_filter} symbols remaining from {current_len}.")

    # 2. Quote Asset Filter
    if quote_asset_filter:
        current_len = len(filtered_symbols_from_api)
        quote_asset_filter_upper = quote_asset_filter.upper()
        filtered_symbols_from_api = [
            s_info for s_info in filtered_symbols_from_api
            if s_info.get("quote_asset", "").upper() == quote_asset_filter_upper
        ]
        count_after_quote_filter = len(filtered_symbols_from_api)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Filtered by quote asset '{quote_asset_filter}': {count_after_quote_filter} symbols remaining from {current_len}.")

    # 3. Instrument Type Filter
    if instrument_type_filter:
        current_len = len(filtered_symbols_from_api)
        types_to_keep = {t.strip().upper() for t in instrument_type_filter.split(',')}
        temp_list = []
        for s_info in filtered_symbols_from_api:
            raw_instrument_type = s_info.get("instrument_type", "").upper()
            base_instrument_type = raw_instrument_type.split('_')[0] if '_' in raw_instrument_type and raw_instrument_type.startswith(FUTURE) else raw_instrument_type
            if base_instrument_type in types_to_keep:
                temp_list.append(s_info)
        filtered_symbols_from_api = temp_list
        count_after_instrument_type_filter = len(filtered_symbols_from_api)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Filtered by instrument types '{instrument_type_filter}': {count_after_instrument_type_filter} symbols remaining from {current_len}.")

    # 4. Symbols List Filter (Standard Symbols)
    if symbols_list_filter:
        current_len = len(filtered_symbols_from_api)
        standard_symbols_to_keep = {s.strip().upper() for s in symbols_list_filter.split(',')}
        temp_list = []
        for s_info in filtered_symbols_from_api:
            try:
                # Construct SymbolRepresentation from API s_info
                s_repr = SymbolRepresentation.from_api_dict(s_info)
                if s_repr.normalized in standard_symbols_to_keep:
                    temp_list.append(s_info)
            except Exception as e:
                logger.warning(f"{'[DRY RUN] ' if dry_run else ''}Could not form SymbolRepresentation for API symbol data {s_info.get('exchange_specific_symbol', s_info)}: {e}")
                continue
        filtered_symbols_from_api = temp_list
        count_after_symbols_list_filter = len(filtered_symbols_from_api)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Filtered by symbols list: {count_after_symbols_list_filter} symbols remaining from {current_len}.")

    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}After all filters, {len(filtered_symbols_from_api)} symbols (out of {original_api_count} from API) will be processed for DB."
    )

    if dry_run:
        logger.info(f"--- [DRY RUN] Symbol Definition Update Summary for {exchange_name.capitalize()} ---")
        logger.info(f"[DRY RUN] Total symbols from API (before any filtering): {original_api_count}")
        if api_status_filter and api_status_filter.upper() not in ["ANY", "NONE"]:
             logger.info(f"[DRY RUN] Symbols remaining after API status filter ('{api_status_filter}'): {count_after_status_filter}")
        if quote_asset_filter:
             logger.info(f"[DRY RUN] Symbols remaining after quote asset filter ('{quote_asset_filter}'): {count_after_quote_filter}")
        if instrument_type_filter:
             logger.info(f"[DRY RUN] Symbols remaining after instrument type filter ('{instrument_type_filter}'): {count_after_instrument_type_filter}")
        if symbols_list_filter:
             logger.info(f"[DRY RUN] Symbols remaining after symbols list filter: {count_after_symbols_list_filter}")
        logger.info(f"[DRY RUN] Total symbols that would be processed for DB: {len(filtered_symbols_from_api)}")
        return -1, -1, -1, -1 # Indicate dry run with placeholder return values

    newly_added_count = 0
    already_exist_count = 0
    skipped_count = 0
    db_error_count = 0

    with db_conn.cursor(cursor_factory=DictCursor) as cursor:
        exchange_id_val = get_or_create_exchange_id(cursor, exchange_name)
        db_conn.commit()

        for i, s_info in enumerate(filtered_symbols_from_api):
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
                    standard_instrument_type,
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
                    exc_info=False,
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
                    f" ...processed {i+1}/{len(filtered_symbols_from_api)} filtered API entries for {exchange_name} definitions."
                )

    logger.info(
        f"--- Symbol Definition Update Summary for {exchange_name.capitalize()} ---"
    )
    logger.info(f"Total symbols from API (before filtering): {original_api_count}, Processed after filtering: {len(filtered_symbols_from_api)}")
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
    parser.add_argument(
        "--quote-asset-filter",
        type=str,
        default=None,
        help="Filter symbols by standardized quote asset (e.g., USDT).",
    )
    parser.add_argument(
        "--api-status-filter",
        type=str,
        default=None,
        help="Filter symbols by API status. Options: TRADING_ONLY, ANY, CUSTOM:STATUS1,STATUS2.",
    )
    parser.add_argument(
        "--instrument-type-filter",
        type=str,
        default=None,
        help="Filter symbols by instrument type (comma-separated, e.g., SPOT,PERP).",
    )
    parser.add_argument(
        "--symbols-list-filter",
        type=str,
        default=None,
        help="Comma-separated list of standard symbols to fetch definitions for (e.g., BTC-USDT,ETH-USD-PERP).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run: fetch and filter symbols but do not write to the database.",
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

            # Load defaults from config if CLI args are not provided
            quote_asset_filter_arg = args.quote_asset_filter or config_obj.get(
                "symbol_management", "default_quote_asset_filter_for_definitions", fallback=None
            )
            api_status_filter_arg = args.api_status_filter or config_obj.get(
                "symbol_management", "default_api_status_filter_for_definitions", fallback="TRADING_ONLY" # Default to TRADING_ONLY
            )
            instrument_type_filter_arg = args.instrument_type_filter or config_obj.get(
                "symbol_management", "default_instrument_type_filter_for_definitions", fallback=None
            )
            symbols_list_filter_arg = args.symbols_list_filter or config_obj.get(
                "symbol_management", "default_symbols_list_filter_for_definitions", fallback=None
            )

            logger.info(f"Applying filters for symbol definitions:")
            logger.info(f"  Quote Asset: {quote_asset_filter_arg or 'Any'}")
            logger.info(f"  API Status: {api_status_filter_arg or 'Any'}")
            logger.info(f"  Instrument Types: {instrument_type_filter_arg or 'Any'}")
            logger.info(f"  Symbols List: {symbols_list_filter_arg or 'None'}")
            
            if symbols_list_filter_arg:
                logger.info("  Note: --symbols-list-filter, if provided, will be the primary filter after basic API fetch; other filters apply to the result of this list or all symbols if list is not provided.")

            if args.dry_run:
                logger.info("*** DRY RUN MODE ENABLED: No changes will be made to the database. ***")


            exchange_adapter_instance = get_exchange_adapter(args.exchange, config_obj)

            if not args.dry_run: # Only connect to DB if not a dry run
                db_connection = get_db_connection(config_obj)
                if db_connection is None:
                    logger.error("Failed to connect to database. Exiting.")
                    exit(1)

            await update_symbol_definitions_from_exchange_api(
                exchange_adapter_instance,
                db_connection,
                quote_asset_filter=quote_asset_filter_arg,
                api_status_filter=api_status_filter_arg,
                instrument_type_filter=instrument_type_filter_arg,
                symbols_list_filter=symbols_list_filter_arg,
                dry_run=args.dry_run,
            )

        except ValueError as ve:
            logger.error(f"Configuration or setup error: {ve}", exc_info=True)
        except Exception as e:
            logger.error(
                f"Main execution error in manage_symbols for {args.exchange.capitalize()}: {e}",
                exc_info=True,
            )
        finally:
            if not args.dry_run: # Only close if it was opened
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