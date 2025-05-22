import argparse
import asyncio
from typing import List, Optional, Dict, Any
import logging

from .utils import (
    load_config,
    get_exchange_adapter,
    DEFAULT_CONFIG_PATH,
    setup_logging,
)
from .exchanges.base_interface import ExchangeInterface

logger = logging.getLogger(__name__)


async def get_top_liquid_symbols_via_adapter(
    adapter: ExchangeInterface,
    top_n: int = 20,
    quote_asset_filter: Optional[str] = "USDT",  # Standard quote asset
    min_volume: float = 0,
) -> List[Dict[str, Any]]:
    logger.info(
        f"Fetching ticker information via {adapter.get_exchange_name()} to identify top {top_n} liquid symbols "
        f"(Standard Quote Filter: {quote_asset_filter or 'Any'}, Min QuoteVolume: {min_volume})..."
    )
    try:
        # Adapter's get_top_liquid_symbols should handle its own cache needs for normalization.
        top_symbols_data = await adapter.get_top_liquid_symbols(
            top_n=top_n,
            standard_quote_asset_filter=quote_asset_filter,
            min_volume=min_volume,
        )
        return top_symbols_data
    except Exception as e:
        logger.error(
            f"Error in get_top_liquid_symbols_via_adapter for {adapter.get_exchange_name()}: {e}",
            exc_info=True,
        )
        return []


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(
        description="List top liquid symbols from an exchange via adapter."
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
        help="Name of the exchange to query (e.g., binance).",
    )
    parser.add_argument(
        "--top-n", type=int, default=20, help="Number of top symbols to list."
    )
    parser.add_argument(
        "--quote-asset",
        type=str,
        default="USDT",
        help="Filter by *standardized* quote asset (e.g., USDT, USD). Leave empty for no filter.",
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=1000000,
        help="Minimum 24h quote asset volume to consider a symbol.",
    )
    args = parser.parse_args()

    async def main_test_cli():
        adapter_instance: Optional[ExchangeInterface] = None
        try:
            config_object = load_config(args.config)
            adapter_instance = get_exchange_adapter(args.exchange, config_object)

            if hasattr(adapter_instance, "_ensure_cache_populated"):
                await adapter_instance._ensure_cache_populated()

        except Exception as e:
            logger.error(f"Error initializing adapter: {e}", exc_info=True)
            exit(1)

        if adapter_instance:
            try:
                std_quote_filter = (
                    args.quote_asset.upper() if args.quote_asset else None
                )
                top_symbols_list_of_dicts = await get_top_liquid_symbols_via_adapter(
                    adapter_instance,
                    top_n=args.top_n,
                    quote_asset_filter=std_quote_filter,
                    min_volume=args.min_volume,
                )

                if top_symbols_list_of_dicts:
                    logger.info(
                        f"\nTop {len(top_symbols_list_of_dicts)} symbols from {args.exchange.upper()} "
                        f"(Std Quote Filter: {std_quote_filter or 'Any'}, Min Vol: {args.min_volume}):"
                    )
                    for idx, sym_data in enumerate(top_symbols_list_of_dicts):
                        print(
                            f"  {idx+1}. Exchange: {sym_data.get('exchange_specific_symbol', 'N/A')}, "
                            f"Standard: {sym_data.get('standard_symbol', 'N/A')}, "
                            f"Volume (Quote Asset): {sym_data.get('normalized_quote_volume', 'N/A'):,.2f}"
                        )
                else:
                    logger.info(
                        f"No symbols found for {args.exchange.upper()} matching the criteria."
                    )
            finally:
                if hasattr(adapter_instance, "close_session") and callable(
                    adapter_instance.close_session
                ):
                    await adapter_instance.close_session()
                    logger.info(f"{args.exchange.capitalize()} adapter session closed.")

    asyncio.run(main_test_cli())
