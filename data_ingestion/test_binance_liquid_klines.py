# File: data_ingestion/test_binance_liquid_klines.py

import argparse
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional

# Assuming your project structure allows these imports
# If running this script directly from the project root, you might need to adjust paths
# or ensure data_ingestion is in PYTHONPATH.
# For simplicity, I'm using relative imports assuming it's in data_ingestion.
try:
    from .utils import load_config, get_exchange_adapter, DEFAULT_CONFIG_PATH
    from .exchanges.base_interface import ExchangeInterface
    from .symbol_utils import get_top_liquid_symbols_via_adapter
    from .exchanges.symbol_representation import (
        SymbolRepresentation,
    )  # Though not directly used, good for context
except ImportError:
    # Fallback for running script directly from data_ingestion folder for testing
    from utils import load_config, get_exchange_adapter, DEFAULT_CONFIG_PATH
    from exchanges.base_interface import ExchangeInterface
    from symbol_utils import get_top_liquid_symbols_via_adapter
    from exchanges.symbol_representation import SymbolRepresentation


async def main_test_script():
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
    # Args for get_top_liquid_symbols
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
        default=1000000,  # Default 1 million
        help="Minimum 24h quote asset volume to consider a symbol.",
    )
    # Args for fetch_klines
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
        "--kline-limit",
        type=int,
        default=10,
        help="Number of klines to fetch.",
    )
    args = parser.parse_args()

    if args.exchange.lower() != "binance":
        print("Error: This script is specifically for the 'binance' adapter.")
        return

    adapter_instance: Optional[ExchangeInterface] = None
    print(f"--- Test Script: Binance Top Liquid Symbol & Klines ---")

    try:
        config_object = load_config(args.config)
        adapter_instance = get_exchange_adapter(args.exchange, config_object)
        print(f"Initialized {args.exchange.capitalize()} adapter.")

        # Ensure cache is populated for normalization if adapter needs it
        # get_top_liquid_symbols_via_adapter calls adapter.get_top_liquid_symbols
        # which itself should call _ensure_cache_populated if designed that way.
        # Let's assume the adapter handles its cache internally for these public methods.

        print(f"\nFetching top {args.top_n_symbols} liquid symbol(s)...")
        top_symbols_data = await get_top_liquid_symbols_via_adapter(
            adapter_instance,
            top_n=args.top_n_symbols,
            quote_asset_filter=args.quote_asset.upper() if args.quote_asset else None,
            min_volume=args.min_volume,
        )

        if not top_symbols_data:
            print("No liquid symbols found matching the criteria.")
            return

        top_symbol_info = top_symbols_data[0]  # Get the most liquid one
        standard_symbol_to_fetch = top_symbol_info.get("standard_symbol")
        exchange_specific_symbol = top_symbol_info.get("exchange_specific_symbol")
        volume = top_symbol_info.get("normalized_quote_volume")

        print(f"\nTop liquid symbol selected:")
        print(f"  Standard: {standard_symbol_to_fetch}")
        print(f"  Exchange-specific: {exchange_specific_symbol}")
        print(f"  Normalized Quote Volume: {volume:,.2f}")

        if not standard_symbol_to_fetch:
            print("Error: Could not determine standard symbol for fetching klines.")
            return

        print(f"\nFetching klines for {standard_symbol_to_fetch}...")
        print(f"  Interval: {args.interval}")
        print(f"  Start Date: {args.start_date}")
        print(f"  End Date: {args.end_date}")
        print(f"  Limit: {args.kline_limit}")

        try:
            start_dt = pd.to_datetime(args.start_date, utc=True)
            end_dt = pd.to_datetime(args.end_date, utc=True) if args.end_date else None
        except Exception as e:
            print(f"Error parsing date arguments: {e}")
            return

        klines_df = await adapter_instance.fetch_klines(
            standard_symbol_str=standard_symbol_to_fetch,
            interval=args.interval,
            start_datetime=start_dt,
            end_datetime=end_dt,
            limit=args.kline_limit,
        )

        if not klines_df.empty:
            print(f"\nFetched {len(klines_df)} klines for {standard_symbol_to_fetch}:")
            print(klines_df.to_string())
        else:
            print(f"\nNo klines returned for {standard_symbol_to_fetch}.")

    except FileNotFoundError as e:
        print(f"Configuration Error: {e}")
    except ValueError as ve:
        print(f"Value Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if adapter_instance and hasattr(adapter_instance, "close_session"):
            await adapter_instance.close_session()
            print(f"\n{args.exchange.capitalize()} adapter session closed.")
        print("\n--- Test Script Finished ---")


if __name__ == "__main__":
    # Ensure the script can find your modules if run directly.
    # If data_ingestion is not in PYTHONPATH, you might need to add it:
    # import sys
    # import os
    # SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # PARENT_DIR = os.path.dirname(SCRIPT_DIR) # Assuming this script is in data_ingestion
    # sys.path.append(PARENT_DIR)

    asyncio.run(main_test_script())
