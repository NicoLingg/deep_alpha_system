import argparse
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional
import logging


try:
    from .utils import (
        load_config,
        get_exchange_adapter,
        DEFAULT_CONFIG_PATH,
        setup_logging,
    )
    from .exchanges.base_interface import ExchangeInterface
    from .symbol_utils import get_top_liquid_symbols_via_adapter
    from .exchanges.symbol_representation import SymbolRepresentation  # Added
except ImportError:
    # Fallback for running script directly from data_ingestion folder for testing
    # This assumes utils.py, etc., are in the same directory or PYTHONPATH is set.
    # For a structured project, it's better to run scripts as modules from the project root.
    import sys, os

    # Add project root to path if running from data_ingestion directly
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT_FROM_SCRIPT = os.path.dirname(SCRIPT_DIR)
    if PROJECT_ROOT_FROM_SCRIPT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT_FROM_SCRIPT)

    from data_ingestion.utils import (
        load_config,
        get_exchange_adapter,
        DEFAULT_CONFIG_PATH,
        setup_logging,
    )
    from data_ingestion.exchanges.base_interface import ExchangeInterface
    from data_ingestion.symbol_utils import get_top_liquid_symbols_via_adapter
    from data_ingestion.exchanges.symbol_representation import SymbolRepresentation


logger = logging.getLogger(__name__)


async def main_test_script():
    setup_logging()  # Initialize logging for this script
    parser = argparse.ArgumentParser(
        description="Test script to get top liquid symbol from Binance and fetch its klines."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to configuration file (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="binance",
        help="Name of the exchange to query (must be 'binance' for this script).",
    )
    parser.add_argument(
        "--top-n-symbols",
        type=int,
        default=10,
        help="Number of top symbols to consider (will pick the very top one).",
    )
    parser.add_argument(
        "--quote-asset",
        type=str,
        default="USDT",
        help="Filter by *standardized* quote asset for liquidity (e.g., USDT, USD).",
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=1000000,
        help="Minimum 24h quote asset volume to consider a symbol.",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default="1m",
        help="Kline interval (e.g., 1m, 5m, 1h, 1d).",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=(datetime.now(timezone.utc) - timedelta(days=7)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        help="Kline start date/datetime (UTC ISO format, e.g., YYYY-MM-DDTHH:MM:SSZ). Default: 7 days ago.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        help="Kline end date/datetime (UTC ISO format). Default: now.",
    )
    parser.add_argument(
        "--kline-limit", type=int, default=10, help="Number of klines to fetch."
    )
    args = parser.parse_args()

    if args.exchange.lower() != "binance":
        logger.error("This script is specifically for the 'binance' adapter.")
        return

    adapter_instance: Optional[ExchangeInterface] = None
    logger.info("--- Test Script: Binance Top Liquid Symbol & Klines ---")

    try:
        config_object = load_config(args.config)
        adapter_instance = get_exchange_adapter(args.exchange, config_object)
        logger.info(f"Initialized {args.exchange.capitalize()} adapter.")

        # Adapter's methods should handle their own cache needs if necessary
        # e.g., get_top_liquid_symbols should call _ensure_cache_populated internally.

        logger.info(f"Fetching top {args.top_n_symbols} liquid symbol(s)...")
        top_symbols_data = await get_top_liquid_symbols_via_adapter(
            adapter_instance,
            top_n=args.top_n_symbols,
            quote_asset_filter=args.quote_asset.upper() if args.quote_asset else None,
            min_volume=args.min_volume,
        )

        if not top_symbols_data:
            logger.warning("No liquid symbols found matching the criteria.")
            return

        top_symbol_info = top_symbols_data[0]
        standard_symbol_to_fetch = top_symbol_info.get("standard_symbol")
        exchange_specific_symbol = top_symbol_info.get("exchange_specific_symbol")
        volume = top_symbol_info.get("normalized_quote_volume")

        logger.info("Top liquid symbol selected:")
        # Using print for result display, not operational logging
        print(f"  Standard: {standard_symbol_to_fetch}")
        print(f"  Exchange-specific: {exchange_specific_symbol}")
        print(f"  Normalized Quote Volume: {volume:,.2f}")

        if not standard_symbol_to_fetch:
            logger.error("Could not determine standard symbol for fetching klines.")
            return

        logger.info(f"Fetching klines for {standard_symbol_to_fetch}...")
        logger.info(
            f"  Interval: {args.interval}, Start: {args.start_date}, End: {args.end_date}, Limit: {args.kline_limit}"
        )

        try:
            start_dt = pd.to_datetime(args.start_date, utc=True)
            end_dt = pd.to_datetime(args.end_date, utc=True) if args.end_date else None
        except Exception as e:
            logger.error(f"Error parsing date arguments: {e}")
            return

        klines_df = await adapter_instance.fetch_klines(
            standard_symbol_str=standard_symbol_to_fetch,
            interval=args.interval,
            start_datetime=start_dt,
            end_datetime=end_dt,
            limit=args.kline_limit,
        )

        if not klines_df.empty:
            logger.info(
                f"Fetched {len(klines_df)} klines for {standard_symbol_to_fetch}:"
            )
            print(klines_df.to_string())  # Print DataFrame for test output
        else:
            logger.warning(f"No klines returned for {standard_symbol_to_fetch}.")

    except FileNotFoundError as e:  # Config file not found
        logger.error(f"Configuration Error: {e}", exc_info=True)
    except ValueError as ve:  # Errors from adapter init, symbol parsing etc.
        logger.error(f"Value Error: {ve}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if adapter_instance and hasattr(adapter_instance, "close_session"):
            await adapter_instance.close_session()
        logger.info("--- Test Script Finished ---")


if __name__ == "__main__":
    asyncio.run(main_test_script())
