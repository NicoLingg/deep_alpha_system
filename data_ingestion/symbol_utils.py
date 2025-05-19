import operator
import argparse

try:
    from .utils import load_config, get_binance_client, DEFAULT_CONFIG_PATH
except ImportError:
    from utils import load_config, get_binance_client, DEFAULT_CONFIG_PATH


def get_top_liquid_symbols(client, top_n=20, quote_asset_filter="USDT", min_volume=0):
    """
    Fetches all tickers and returns the top N most liquid symbols
    based on 24-hour quote asset volume, optionally filtered by a quote asset and minimum volume.
    """
    print(
        f"Fetching ticker information to identify top {top_n} liquid symbols "
        f"(Quote Filter: {quote_asset_filter or 'Any'}, Min QuoteVolume: {min_volume})..."
    )
    try:
        tickers = client.get_ticker()  # Fetches all 24hr ticker price change statistics
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return []

    liquid_symbols_data = []
    for ticker in tickers:
        symbol = ticker["symbol"]
        try:
            quote_volume = float(ticker["quoteVolume"])
            if quote_volume < min_volume:
                continue

            if quote_asset_filter:
                if symbol.endswith(quote_asset_filter):
                    liquid_symbols_data.append(
                        {"symbol": symbol, "quoteVolume": quote_volume}
                    )
            else:
                liquid_symbols_data.append(
                    {"symbol": symbol, "quoteVolume": quote_volume}
                )
        except (KeyError, ValueError):
            continue

    liquid_symbols_data.sort(key=operator.itemgetter("quoteVolume"), reverse=True)

    return [item["symbol"] for item in liquid_symbols_data[:top_n]]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="List top liquid symbols from Binance."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to configuration file (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--top_n", type=int, default=20, help="Number of top symbols to list."
    )
    parser.add_argument(
        "--quote_asset",
        type=str,
        default="USDT",
        help="Filter by quote asset (e.g., USDT, BTC). Leave empty for no filter.",
    )
    parser.add_argument(
        "--min_volume",
        type=float,
        default=1000000,  # Example: 1 Million USD(T) volume
        help="Minimum 24h quote asset volume to consider a symbol.",
    )

    args = parser.parse_args()

    try:
        config_object = load_config(args.config)
    except FileNotFoundError as e:
        print(e)
        exit(1)
    except KeyError as e:
        print(f"Configuration error: {e}")
        exit(1)

    client = get_binance_client(config_object)
    if client:
        quote_filter = args.quote_asset if args.quote_asset else None

        top_symbols = get_top_liquid_symbols(
            client,
            top_n=args.top_n,
            quote_asset_filter=quote_filter,
            min_volume=args.min_volume,
        )

        if top_symbols:
            print(
                f"\nTop {len(top_symbols)} symbols by 24h Quote Volume (Filter: {quote_filter or 'Any'}, Min Vol: {args.min_volume}):"
            )
            for sym in top_symbols:
                print(sym)
        else:
            print("No symbols found matching the criteria.")
