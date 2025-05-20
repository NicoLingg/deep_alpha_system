import argparse
import psycopg2
from binance.exceptions import BinanceAPIException

from .utils import (
    load_config,
    get_db_connection,
    get_binance_client,
    get_or_create_exchange_id,
    get_or_create_symbol_id,
    DEFAULT_CONFIG_PATH,
)


def update_symbol_definitions_from_binance_api(
    b_client,
    db_conn,
    target_statuses,
    target_permissions_hint,
    target_quote_asset=None,
):
    print("Stage 1: Updating symbol definitions in local DB from Binance API...")
    statuses_log = (
        target_statuses
        if target_statuses
        else "Any (if empty list means no status filter)"
    )
    permissions_hint_log = (
        target_permissions_hint
        if target_permissions_hint
        else "Any (relying on isSpotTradingAllowed if SPOT is hinted)"
    )
    print(
        f"Filters: Statuses={statuses_log}, Permissions Hint={permissions_hint_log}, QuoteAsset={target_quote_asset or 'Any'}"
    )

    try:
        exchange_info = b_client.get_exchange_info()
    except BinanceAPIException as e:
        print(f"API Error fetching exchange info: {e}")
        return 0, 0, 0, 0
    except Exception as e:
        print(f"Unexpected error fetching exchange info: {e}")
        return 0, 0, 0, 0

    if not exchange_info or "symbols" not in exchange_info:
        print("No symbols data found in Binance exchange information.")
        return 0, 0, 0, 0

    all_api_symbols = exchange_info["symbols"]
    print(f"Received {len(all_api_symbols)} symbol entries from Binance API.")

    newly_added_count = 0
    already_exist_count = 0
    skipped_by_filter_count = 0
    db_error_count = 0
    processed_for_debug = 0

    with db_conn.cursor() as cursor:
        exchange_id = get_or_create_exchange_id(cursor, "Binance")
        db_conn.commit()

        for i, s_info in enumerate(all_api_symbols):
            instrument_name = s_info.get("symbol")
            base_asset = s_info.get("baseAsset")
            quote_asset_api = s_info.get("quoteAsset")
            status_api = s_info.get("status")
            is_spot_trading_allowed = s_info.get("isSpotTradingAllowed", False)

            # ---- START DEBUG BLOCK ----
            # if instrument_name == "BTCUSDT" or instrument_name == "NOTUSDT" or (target_quote_asset and quote_asset_api == target_quote_asset and processed_for_debug < 2) :
            #     print(f"\n[DEBUG] Processing {instrument_name}:")
            #     print(f"  API Status: {status_api}, API isSpotTradingAllowed: {is_spot_trading_allowed}, API Quote: {quote_asset_api}")
            #     print(f"  Target Statuses: {target_statuses}")
            #     print(f"  Target Permissions Hint: {target_permissions_hint}")
            #     print(f"  Target Quote: {target_quote_asset}")
            # ---- END DEBUG BLOCK ----

            if not all([instrument_name, base_asset, quote_asset_api, status_api]):
                skipped_by_filter_count += 1
                continue

            if target_statuses and status_api not in target_statuses:
                skipped_by_filter_count += 1
                continue

            passes_permission_check = True
            if target_permissions_hint:
                if "SPOT" in target_permissions_hint and not is_spot_trading_allowed:
                    passes_permission_check = False

            if not passes_permission_check:
                skipped_by_filter_count += 1
                continue

            if target_quote_asset and quote_asset_api != target_quote_asset:
                skipped_by_filter_count += 1
                continue

            try:
                cursor.execute(
                    "SELECT symbol_id FROM symbols WHERE exchange_id = %s AND instrument_name = %s",
                    (exchange_id, instrument_name),
                )
                existing_symbol_row = cursor.fetchone()

                _symbol_id = get_or_create_symbol_id(
                    cursor, exchange_id, instrument_name, base_asset, quote_asset_api
                )
                db_conn.commit()

                if not existing_symbol_row:
                    newly_added_count += 1
                else:
                    already_exist_count += 1

            except psycopg2.Error as db_e:
                db_conn.rollback()
                print(f"DB Error processing symbol {instrument_name}: {db_e}")
                db_error_count += 1
            except Exception as e_upsert:
                db_conn.rollback()
                print(
                    f"Unexpected Error during get_or_create for symbol {instrument_name}: {e_upsert}"
                )
                db_error_count += 1

            if (i + 1) % 500 == 0:
                print(
                    f" ...processed {i+1}/{len(all_api_symbols)} API entries for definitions."
                )

    print("\n--- Symbol Definition Update Summary ---")
    print(f"Total symbols from Binance API: {len(all_api_symbols)}")
    print(f"Skipped by filters: {skipped_by_filter_count}")
    print(f"New symbols added to DB: {newly_added_count}")
    print(f"Symbols already existing in DB: {already_exist_count}")
    print(f"DB errors during processing: {db_error_count}")
    return (
        newly_added_count,
        already_exist_count,
        skipped_by_filter_count,
        db_error_count,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage 1: Updates local symbol definitions from Binance API."
    )
    parser.add_argument(
        "--config", type=str, default=DEFAULT_CONFIG_PATH, help="Path to config file."
    )
    parser.add_argument(
        "--include-statuses",
        type=str,
        default=None,
        help="Comma-separated symbol statuses to include. Default from config.",
    )
    parser.add_argument(
        "--include-permissions-hint",
        type=str,
        default=None,
        help="Comma-separated permissions HINT (e.g., SPOT,MARGIN). Primarily uses boolean flags like isSpotTradingAllowed. Default from config.",
    )
    parser.add_argument(
        "--quote-asset-filter",
        type=str,
        default=None,
        help="Only process symbols with this quote asset. No filter if not set.",
    )

    args = parser.parse_args()
    config = load_config(args.config)

    statuses_str_cli = args.include_statuses
    statuses_str_config = config.get(
        "symbol_management", "default_include_statuses", fallback=None
    )
    final_statuses_str = (
        statuses_str_cli if statuses_str_cli is not None else statuses_str_config
    )
    target_statuses_list = (
        [s.strip().upper() for s in final_statuses_str.split(",") if s.strip()]
        if final_statuses_str
        else []
    )

    permissions_hint_str_cli = args.include_permissions_hint
    permissions_hint_str_config = config.get(
        "symbol_management", "default_include_permissions", fallback=None
    )
    final_permissions_hint_str = (
        permissions_hint_str_cli
        if permissions_hint_str_cli is not None
        else permissions_hint_str_config
    )
    target_permissions_hint_list = (
        [p.strip().upper() for p in final_permissions_hint_str.split(",") if p.strip()]
        if final_permissions_hint_str
        else []
    )

    quote_asset_arg = args.quote_asset_filter

    if (
        not target_statuses_list
        and statuses_str_cli is None
        and (statuses_str_config is None or statuses_str_config == "")
    ):
        print(
            "Info: No 'include-statuses' from CLI, and config 'default_include_statuses' is empty or missing. Applying NO status filter."
        )
    if (
        not target_permissions_hint_list
        and permissions_hint_str_cli is None
        and (permissions_hint_str_config is None or permissions_hint_str_config == "")
    ):
        print(
            "Info: No 'include-permissions-hint' from CLI, and config 'default_include_permissions' is empty or missing. Permission check will be less strict or rely on defaults within processing function."
        )
    if not quote_asset_arg:
        print("Info: No 'quote-asset-filter' provided. Applying NO quote asset filter.")

    db_conn = None
    try:
        b_client = get_binance_client(config)
        db_conn = get_db_connection(config)

        update_symbol_definitions_from_binance_api(
            b_client,
            db_conn,
            target_statuses_list,
            target_permissions_hint_list,
            quote_asset_arg,
        )
        print("\nSymbol definition update script finished.")
    except Exception as e:
        print(f"Main execution error in manage_symbols: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if db_conn:
            db_conn.close()
            print("Database connection closed.")
