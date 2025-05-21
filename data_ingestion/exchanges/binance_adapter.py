import asyncio
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional
from binance.client import Client as BinanceSyncClient
from binance.exceptions import BinanceAPIException, BinanceRequestException

from .base_interface import ExchangeInterface
from .symbol_representation import SymbolRepresentation, SPOT, PERP, FUTURE

# Standardized internal quote asset concepts mapped FROM Binance's typical quote assets.
# Example: If Binance uses "BUSD" or "USDC" for some pairs, and your internal standard is "USDT"
# for all of them, or a more generic "USD" for USD-pegged stablecoins.
# This example maps BUSD and USDC to a standard of "USDT". Adjust as needed.
# If your standard is "USD", then map "USDT", "BUSD", "USDC" all to "USD".
BINANCE_QUOTE_TO_STANDARD_MAP = {
    "USDT": "USDT",
    "BUSD": "USDT",  # Map BUSD to standard USDT
    "USDC": "USDT",  # Map USDC to standard USDT
    "TUSD": "USDT",
    "PAX": "USDT",  # Paxos Standard was renamed to Pax Dollar (USDP)
    "USDP": "USDT",  # Map USDP to standard USDT
    "DAI": "DAI",  # DAI might be its own standard or mapped to USD/USDT
    "BTC": "BTC",
    "ETH": "ETH",
    # For COIN-M futures/perps, the quote is often the base (e.g. BTC for BTCUSD_PERP)
    # but the underlying concept might be USD.
    # This map is for standardizing the quote asset string part.
}

# Map your STANDARD quote asset concept TO what Binance uses, especially for PERPs/FUTURES
# E.g., if your standard is "USD-PERP" and Binance uses USDT-margined perps for this.
STANDARD_QUOTE_TO_BINANCE_PERP_QUOTE_MAP = {
    "USD": "USDT",  # Standard "USD" for perps means Binance "USDT" (for USDT-M)
    "USDT": "USDT",  # Standard "USDT" for perps means Binance "USDT"
    # For COIN-M perps, if standard is "BTC-PERP" (meaning BTC is the quote conceptually for inversion)
    # then Binance symbol would be something like BTCUSD_PERP, where USD is the conceptual underlying for price.
    # This map is more for margin currency.
}


class BinanceAdapter(ExchangeInterface):
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        config_override: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(api_key, api_secret, config_override)
        self.tld = self.config_override.get("tld", "com")
        self.testnet = self.config_override.get("testnet", "false").lower() == "true"

        self.sync_client = BinanceSyncClient(
            api_key, api_secret, tld=self.tld, testnet=self.testnet
        )
        self._exchange_info_list: Optional[List[Dict[str, Any]]] = (
            None  # For fetch_exchange_symbols_info
        )
        self._symbol_details_cache: Dict[str, Dict[str, Any]] = (
            {}
        )  # exchange_specific_symbol.upper() -> standardized_details_dict
        self._ensure_cache_task: Optional[asyncio.Task] = None

    async def _ensure_cache_populated(self, force_refresh: bool = False):
        # Simple single-flight mechanism for concurrent calls
        if self._ensure_cache_task and not self._ensure_cache_task.done():
            await self._ensure_cache_task
            return

        if self._exchange_info_list is None or force_refresh:
            self._ensure_cache_task = asyncio.create_task(
                self._refresh_exchange_info_cache_impl()
            )
            try:
                await self._ensure_cache_task
            finally:
                self._ensure_cache_task = None

    async def _refresh_exchange_info_cache_impl(self):
        print(
            f"[{self.get_exchange_name().capitalize()}] Fetching/Refreshing exchange symbols info cache..."
        )
        try:
            # Use asyncio.to_thread for the blocking call
            raw_exchange_info = await asyncio.to_thread(
                self.sync_client.get_exchange_info
            )
        except (BinanceAPIException, BinanceRequestException) as e:
            print(f"Binance API error fetching exchange info: {e}")
            self._exchange_info_list = []
            self._symbol_details_cache = {}
            return
        except Exception as e_generic:
            print(f"Generic error fetching Binance exchange info: {e_generic}")
            self._exchange_info_list = []
            self._symbol_details_cache = {}
            return

        symbols_data_list = []
        temp_symbol_details_cache = {}

        for s_info in raw_exchange_info.get("symbols", []):
            exchange_specific_symbol = s_info["symbol"]
            binance_base_asset = s_info["baseAsset"]
            binance_quote_asset = s_info["quoteAsset"]

            # Standardize assets
            standard_base_asset = (
                binance_base_asset.upper()
            )  # Base assets usually don't need complex mapping
            standard_quote_asset = BINANCE_QUOTE_TO_STANDARD_MAP.get(
                binance_quote_asset.upper(), binance_quote_asset.upper()
            )

            # Determine instrument type
            instrument_type = SPOT
            if s_info.get("contractType") == "PERPETUAL":
                instrument_type = PERP
                # For USDT-margined perps (e.g., BTCUSDT), if your standard is "USD" for the quote concept.
                if standard_quote_asset == "USDT":
                    standard_quote_asset = "USD"  # Standardize to conceptual "USD"
            elif (
                s_info.get("contractType")
                and s_info.get("contractType") != "NONE"
                and s_info.get("contractType") != "SPOT"
            ):  # Dated futures
                # contractType can be CURRENT_QUARTER, NEXT_QUARTER, etc.
                # Symbol name (e.g., BTCUSD_240927) is more reliable for date for COIN-M.
                # USDT-M futures (less common than perps) might be like BTCUSDTM240927
                if "_" in exchange_specific_symbol:  # Usually COIN-M like BTCUSD_YYMMDD
                    parts = exchange_specific_symbol.split("_")
                    date_part = parts[-1]
                    if date_part.isdigit() and len(date_part) == 6:  # YYMMDD
                        instrument_type = f"{FUTURE}_{date_part}"
                        # COIN-M futures are quoted against USD conceptually, e.g. BTCUSD_240927
                        if exchange_specific_symbol.startswith(
                            standard_base_asset + "USD_"
                        ):
                            standard_quote_asset = "USD"
                # Add logic for USDT-M futures if needed, they have different symbol format usually

            tick_size = Decimal("0")
            lot_size = Decimal("0")
            min_notional = Decimal("0")

            for f_filter in s_info.get("filters", []):
                if f_filter["filterType"] == "PRICE_FILTER":
                    tick_size = Decimal(str(f_filter["tickSize"]))
                elif f_filter["filterType"] == "LOT_SIZE":
                    lot_size = Decimal(str(f_filter["stepSize"]))
                elif (
                    f_filter["filterType"] == "MIN_NOTIONAL"
                ):  # Spot, USDT-M Futures/Perps
                    min_notional = Decimal(str(f_filter.get("notional", "0")))
                elif (
                    f_filter["filterType"] == "NOTIONAL"
                ):  # Spot (older name, less common)
                    min_notional = Decimal(str(f_filter.get("minNotional", "0")))

            symbol_detail_for_list = {
                "exchange_specific_symbol": exchange_specific_symbol,
                "base_asset": standard_base_asset,
                "quote_asset": standard_quote_asset,
                "instrument_type": instrument_type,
                "status": s_info.get("status", "UNKNOWN").upper(),
                "tick_size": tick_size,
                "lot_size": lot_size,
                "min_notional": min_notional,
                "raw_details": s_info,
            }
            symbols_data_list.append(symbol_detail_for_list)
            temp_symbol_details_cache[exchange_specific_symbol.upper()] = (
                symbol_detail_for_list
            )

        self._exchange_info_list = symbols_data_list
        self._symbol_details_cache = temp_symbol_details_cache
        print(
            f"[{self.get_exchange_name().capitalize()}] Cache populated with {len(self._exchange_info_list)} symbols."
        )

    def get_exchange_name(self) -> str:
        return "binance"

    def normalize_standard_symbol_to_exchange(self, standard_symbol_str: str) -> str:
        # Ensure cache is populated. This is a sync method, so can't await here.
        # Caller should ensure cache is populated or handle potential errors.
        # A more robust design might make this async or require explicit cache loading.
        if not self._symbol_details_cache:
            # This is a fallback, direct string manipulation, less robust than cache lookup.
            # print(f"Warning: Binance _symbol_details_cache is empty during normalize_standard_symbol_to_exchange for {standard_symbol_str}. Attempting direct conversion.")
            try:
                s_repr = SymbolRepresentation.parse(standard_symbol_str)
                if s_repr.instrument_type == SPOT:
                    # For SPOT, find a Binance quote that maps to the standard quote. This is complex.
                    # Simplistic: assume direct concatenation if standard quote is common like USDT.
                    # Example: BTC-USDT (standard) -> BTCUSDT (Binance)
                    # If standard is BTC-USD and Binance uses BTCBUSD for that, this is tricky without cache.
                    # For now, assume standard_quote is directly usable.
                    return f"{s_repr.base_asset}{s_repr.quote_asset}"
                elif s_repr.instrument_type == PERP:
                    binance_margin_asset = STANDARD_QUOTE_TO_BINANCE_PERP_QUOTE_MAP.get(
                        s_repr.quote_asset, s_repr.quote_asset
                    )
                    return f"{s_repr.base_asset}{binance_margin_asset}"  # e.g. BTCUSDT for BTC-USD-PERP
                elif s_repr.instrument_type == FUTURE and s_repr.expiry_date:
                    # Standard: BTC-USD-FUTURE_YYMMDD -> Binance COIN-M: BTCUSD_YYMMDD
                    # Standard: BTC-USDT-FUTURE_YYMMDD -> Binance USDT-M: BTCUSDTYYMMDD (or similar)
                    # This part needs careful handling based on Binance's exact naming for USDT-M futures
                    if s_repr.quote_asset == "USD":  # Assume COIN-M like BTCUSD_YYMMDD
                        return f"{s_repr.base_asset}{s_repr.quote_asset}_{s_repr.expiry_date}"
                    # Add USDT-M futures logic if standard includes them
                    else:  # Assuming USDT margined for other quotes, which may not exist or have diff format
                        return f"{s_repr.base_asset}{s_repr.quote_asset}{s_repr.expiry_date}"  # Guessing format
                else:
                    raise ValueError(
                        f"BinanceAdapter: Fallback normalization failed for unsupported type in '{standard_symbol_str}'."
                    )

            except ValueError as e:
                raise ValueError(
                    f"BinanceAdapter: Error parsing standard symbol '{standard_symbol_str}' for exchange normalization: {e}. Cache might be needed."
                )

        # Preferred way: Use cache by reversing the lookup
        try:
            s_repr = SymbolRepresentation.parse(standard_symbol_str)
        except ValueError as e:
            raise ValueError(
                f"BinanceAdapter: Invalid standard symbol string '{standard_symbol_str}': {e}"
            )

        for ex_sym, details in self._symbol_details_cache.items():
            if (
                details["base_asset"] == s_repr.base_asset
                and details["quote_asset"] == s_repr.quote_asset
            ):
                # Compare normalized instrument types (SymbolRepresentation handles FUTURE_YYMMDD)
                cached_s_repr = SymbolRepresentation(
                    base_asset=details["base_asset"],
                    quote_asset=details["quote_asset"],
                    instrument_type=details["instrument_type"],
                    expiry_date=(
                        details["instrument_type"].split("_")[1]
                        if details["instrument_type"].startswith(f"{FUTURE}_")
                        else None
                    ),
                )
                if cached_s_repr.instrument_type == s_repr.instrument_type:
                    if s_repr.instrument_type == FUTURE:
                        if cached_s_repr.expiry_date == s_repr.expiry_date:
                            return ex_sym
                    else:  # SPOT, PERP
                        return ex_sym

        # Fallback if not found in cache (should be rare if cache is comprehensive and recently refreshed)
        # This means the specific combination of base, quote, type isn't directly in our cache.
        # Try the direct conversion logic again as a last resort (might indicate an incomplete cache or new symbol)
        # print(f"Warning: Symbol {standard_symbol_str} not found in Binance cache by component match. Trying direct fallback conversion.")
        try:
            s_repr_fallback = SymbolRepresentation.parse(
                standard_symbol_str
            )  # Renamed to avoid conflict
            if s_repr_fallback.instrument_type == SPOT:
                return f"{s_repr_fallback.base_asset}{s_repr_fallback.quote_asset}"
            elif s_repr_fallback.instrument_type == PERP:
                binance_margin_asset = STANDARD_QUOTE_TO_BINANCE_PERP_QUOTE_MAP.get(
                    s_repr_fallback.quote_asset, s_repr_fallback.quote_asset
                )
                return f"{s_repr_fallback.base_asset}{binance_margin_asset}"
            elif (
                s_repr_fallback.instrument_type == FUTURE
                and s_repr_fallback.expiry_date
            ):
                if s_repr_fallback.quote_asset == "USD":  # COIN-M
                    return f"{s_repr_fallback.base_asset}{s_repr_fallback.quote_asset}_{s_repr_fallback.expiry_date}"
                else:  # USDT-M (Binance format might vary, e.g. BTCUSDT240927)
                    return f"{s_repr_fallback.base_asset}{s_repr_fallback.quote_asset}{s_repr_fallback.expiry_date}"
        except Exception as e:
            raise ValueError(
                f"BinanceAdapter: Unable to normalize standard symbol '{standard_symbol_str}' to exchange format. Error: {e}. Consider refreshing cache."
            )

        raise ValueError(
            f"BinanceAdapter: Could not find exchange symbol for standard '{standard_symbol_str}' in cache and fallback failed."
        )

    def normalize_exchange_symbol_to_standard(
        self, exchange_specific_symbol: str, instrument_type_hint: Optional[str] = None
    ) -> str:
        # Ensure cache is populated (sync context, problematic if not pre-populated)
        if not self._symbol_details_cache:
            # This should ideally not happen in normal operation if fetch_exchange_symbols_info
            # is called by manage_symbols.py before other operations.
            # Awaiting here is not possible. Caller must ensure cache is ready or this method must be async.
            # Forcing it to be async to align with _ensure_cache_populated pattern
            raise RuntimeError(
                "BinanceAdapter: _symbol_details_cache not populated. Call ensure_cache_populated() first if using this method in a context where it might be empty."
            )

        details = self._symbol_details_cache.get(exchange_specific_symbol.upper())
        if not details:
            raise ValueError(
                f"BinanceAdapter: Exchange symbol '{exchange_specific_symbol}' not found in cache. "
                f"Hint: '{instrument_type_hint}'. Ensure cache is up-to-date."
            )

        s_repr = SymbolRepresentation(
            base_asset=details["base_asset"],
            quote_asset=details["quote_asset"],
            instrument_type=details["instrument_type"],
            # expiry_date might need to be extracted if instrument_type is generic like "FUTURE"
            # but current _refresh_exchange_info_cache_impl stores specific "FUTURE_YYMMDD"
            expiry_date=(
                details["instrument_type"].split("_")[1]
                if details["instrument_type"].startswith(
                    f"{FUTURE}_"
                )  # Use f-string for FUTURE
                else None
            ),
        )
        return s_repr.normalized

    async def fetch_exchange_symbols_info(self) -> List[Dict[str, Any]]:
        await self._ensure_cache_populated()
        return self._exchange_info_list if self._exchange_info_list is not None else []

    async def fetch_klines(
        self,
        standard_symbol_str: str,
        interval: str,
        start_datetime: Optional[pd.Timestamp] = None,
        end_datetime: Optional[pd.Timestamp] = None,
        limit: Optional[int] = 1000,
    ) -> pd.DataFrame:
        await self._ensure_cache_populated()  # Ensure mappings are available for normalization
        exchange_symbol = self.normalize_standard_symbol_to_exchange(
            standard_symbol_str
        )

        api_start_str = (
            start_datetime.strftime("%d %b, %Y %H:%M:%S UTC")
            if start_datetime
            else None
        )
        api_end_str = (
            end_datetime.strftime("%d %b, %Y %H:%M:%S UTC") if end_datetime else None
        )

        try:
            klines_data = await asyncio.to_thread(
                self.sync_client.get_historical_klines,
                exchange_symbol,
                interval,
                start_str=api_start_str,
                end_str=api_end_str,
                limit=limit if limit else 1000,  # Max limit is 1000 for Binance
            )
        except (BinanceAPIException, BinanceRequestException) as e:
            print(
                f"Binance API error fetching klines for {exchange_symbol} ({standard_symbol_str}): {e}"
            )
            return pd.DataFrame()
        except Exception as e_generic:
            print(
                f"Generic error fetching klines for {exchange_symbol} ({standard_symbol_str}): {e_generic}"
            )
            return pd.DataFrame()

        if not klines_data:
            return pd.DataFrame()

        # Define the standard columns expected by the system
        standard_columns_output_order = [
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "close_timestamp",
            "trade_count",
            "taker_base_volume",
            "taker_quote_volume",
        ]

        # Initialize an empty DataFrame with these columns if klines_data is empty
        # This case is handled by `if not klines_data: return pd.DataFrame()` above,
        # but if we wanted to return a DF with specific columns, this is how:
        # if not klines_data:
        #     empty_df = pd.DataFrame(columns=standard_columns_output_order)
        #     # Ensure correct dtypes for an empty DataFrame if necessary
        #     for col in standard_columns_output_order:
        #         if col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume", "taker_quote_volume"]:
        #             empty_df[col] = pd.Series(dtype=object) # For Decimals
        #         elif col in ["time", "close_timestamp"]:
        #             empty_df[col] = pd.Series(dtype='datetime64[ns, UTC]')
        #         elif col == "trade_count":
        #             empty_df[col] = pd.Series(dtype=pd.Int64Dtype())
        #     return empty_df

        df = pd.DataFrame(
            klines_data,
            columns=[
                "kline_open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "kline_close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "ignore",
            ],
        )

        # Time conversions
        df["time"] = pd.to_datetime(df["kline_open_time"], unit="ms", utc=True)
        df["close_timestamp"] = pd.to_datetime(
            df["kline_close_time"], unit="ms", utc=True
        )

        # Rename to standard column names
        df.rename(
            columns={
                "quote_asset_volume": "quote_volume",
                "number_of_trades": "trade_count",
                "taker_buy_base_asset_volume": "taker_base_volume",
                "taker_buy_quote_asset_volume": "taker_quote_volume",
            },
            inplace=True,
        )

        # Ensure all standard columns are present, fill with pd.NA if missing from source
        # This is important if other exchanges don't provide all these fields
        for col_name in standard_columns_output_order:
            if col_name not in df.columns:
                if col_name == "trade_count":
                    df[col_name] = pd.Series([pd.NA] * len(df), dtype=pd.Int64Dtype())
                elif col_name in [
                    "taker_base_volume",
                    "taker_quote_volume",
                    "quote_volume",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ]:  # Decimals can be object type holding None/Decimal
                    df[col_name] = pd.Series([None] * len(df), dtype=object)
                elif col_name in [
                    "time",
                    "close_timestamp",
                ]:  # Should always be present from Binance
                    pass  # Assuming these are always derived
                else:
                    df[col_name] = pd.NA

        # Numeric conversions (Decimal and Int64 for nullable int)
        decimal_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "taker_base_volume",
            "taker_quote_volume",
        ]
        for col in decimal_cols:
            if col in df.columns:
                # Convert to string first to handle potential scientific notation correctly with Decimal
                df[col] = (
                    df[col]
                    .astype(str)
                    .apply(
                        lambda x: (
                            Decimal(x)
                            if x not in ["None", "nan", "NaT", str(pd.NA)]
                            else None
                        )
                    )
                )
            else:  # Should have been created above with None
                df[col] = pd.Series([None] * len(df), dtype=object)

        if "trade_count" in df.columns:
            df["trade_count"] = pd.to_numeric(
                df["trade_count"], errors="coerce"
            ).astype(pd.Int64Dtype())
        else:  # Should have been created above with pd.NA
            df["trade_count"] = pd.Series([pd.NA] * len(df), dtype=pd.Int64Dtype())

        # Select and reorder to standard
        return df[standard_columns_output_order]

    async def fetch_orderbook_snapshot(
        self, standard_symbol_str: str, limit: int = 100
    ) -> Dict[str, Any]:
        await self._ensure_cache_populated()
        exchange_symbol = self.normalize_standard_symbol_to_exchange(
            standard_symbol_str
        )

        valid_limits = [
            5,
            10,
            20,
            50,
            100,
            500,
            1000,
            5000,
        ]  # Spot limits, Futures might differ slightly
        actual_limit = limit
        if limit not in valid_limits:
            actual_limit = next(
                (l for l in valid_limits if l >= limit), valid_limits[-1]
            )

        try:
            depth = await asyncio.to_thread(
                self.sync_client.get_order_book,
                symbol=exchange_symbol,
                limit=actual_limit,
            )
        except (BinanceAPIException, BinanceRequestException) as e:
            print(
                f"Binance API error fetching orderbook for {exchange_symbol} ({standard_symbol_str}): {e}"
            )
            return {"bids": [], "asks": [], "lastUpdateId": None, "exchange_ts": None}
        except Exception as e_generic:
            print(
                f"Generic error fetching orderbook for {exchange_symbol} ({standard_symbol_str}): {e_generic}"
            )
            return {"bids": [], "asks": [], "lastUpdateId": None, "exchange_ts": None}

        exchange_timestamp_ms = depth.get(
            "T", depth.get("E")
        )  # 'T' for SPOT, 'E' for FUTURES event time
        exchange_pd_ts: Optional[pd.Timestamp] = (
            pd.to_datetime(exchange_timestamp_ms, unit="ms", utc=True)
            if exchange_timestamp_ms is not None  # Ensure not None before conversion
            else None
        )

        return {
            "bids": [
                [Decimal(str(b[0])), Decimal(str(b[1]))] for b in depth.get("bids", [])
            ],
            "asks": [
                [Decimal(str(a[0])), Decimal(str(a[1]))] for a in depth.get("asks", [])
            ],
            "lastUpdateId": depth.get("lastUpdateId"),
            "exchange_ts": exchange_pd_ts,
        }

    async def check_api_symbol_status(
        self, exchange_specific_symbol: str
    ) -> Dict[str, Any]:
        try:
            s_info = await asyncio.to_thread(
                self.sync_client.get_symbol_info, exchange_specific_symbol
            )
            if not s_info:
                return {
                    "status": "UNKNOWN",
                    "is_trading_allowed": False,
                    "error_message": "No symbol info from API.",
                    "raw_details": None,
                }

            api_status = s_info.get("status", "UNKNOWN").upper()
            is_trading_allowed = api_status == "TRADING"

            # Specific checks for spot vs futures
            if "isSpotTradingAllowed" in s_info:  # SPOT
                is_trading_allowed = is_trading_allowed and s_info.get(
                    "isSpotTradingAllowed", False
                )
            elif "contractType" in s_info:  # FUTURES/PERP
                contract_status = s_info.get("contractStatus", "").upper()
                if contract_status and contract_status != "TRADING":
                    is_trading_allowed = False

            return {
                "status": api_status,
                "is_trading_allowed": is_trading_allowed,
                "raw_details": s_info,
            }
        except (BinanceAPIException, BinanceRequestException) as e:
            return {
                "status": (
                    "ERROR" if e.code != -1121 else "UNKNOWN"
                ),  # -1121 is "Invalid symbol"
                "is_trading_allowed": False,
                "error_message": str(e),
                "error_code": e.code,
                "raw_details": None,
            }
        except Exception as e_generic:
            print(
                f"Generic error checking API status for {exchange_specific_symbol}: {e_generic}"
            )
            return {
                "status": "ERROR",
                "is_trading_allowed": False,
                "error_message": str(e_generic),
                "raw_details": None,
            }

    async def get_top_liquid_symbols(
        self,
        top_n: int = 20,
        standard_quote_asset_filter: Optional[str] = "USDT",
        min_volume: float = 0,
    ) -> List[Dict[str, Any]]:
        await self._ensure_cache_populated()  # Critical for normalization

        try:
            # This fetches SPOT tickers. For futures, use client.futures_ticker() or client.delivery_ticker()
            # A robust adapter would check symbol type / endpoint based on config or standard_symbol structure.
            # Assuming SPOT for this example as `get_ticker` is for SPOT.
            tickers = await asyncio.to_thread(self.sync_client.get_ticker)
        except (BinanceAPIException, BinanceRequestException) as e:
            print(f"Error fetching tickers from Binance: {e}")
            return []
        except Exception as e_generic:
            print(f"Generic error fetching tickers from Binance: {e_generic}")
            return []

        liquid_symbols_data = []
        for ticker_data in tickers:
            exchange_symbol = ticker_data["symbol"]

            # Use cached details for standardization
            cached_details = self._symbol_details_cache.get(exchange_symbol.upper())
            if not cached_details:
                # print(f"Skipping ticker {exchange_symbol}: Not found in cached exchange info.")
                continue

            # Filter by instrument type (e.g. only SPOT if that's what get_ticker returns)
            # For this example, assuming get_ticker() returns SPOT symbols.
            if (
                cached_details["instrument_type"] != SPOT
            ):  # Or PERP, FUTURE if using futures_ticker
                continue

            if standard_quote_asset_filter:
                if (
                    cached_details["quote_asset"].upper()
                    != standard_quote_asset_filter.upper()
                ):
                    continue

            try:
                quote_volume_val = float(ticker_data["quoteVolume"])
                if quote_volume_val < min_volume:
                    continue

                s_repr = SymbolRepresentation(
                    base_asset=cached_details["base_asset"],
                    quote_asset=cached_details["quote_asset"],
                    instrument_type=cached_details["instrument_type"],
                    # Expiry etc. would be needed if handling futures here
                    expiry_date=(
                        cached_details["instrument_type"].split("_")[1]
                        if cached_details["instrument_type"].startswith(f"{FUTURE}_")
                        else None
                    ),
                )
                standard_symbol_str = s_repr.normalized

                liquid_symbols_data.append(
                    {
                        "exchange_specific_symbol": exchange_symbol,
                        "standard_symbol": standard_symbol_str,
                        "normalized_quote_volume": quote_volume_val,
                        "raw_ticker_data": ticker_data,
                    }
                )
            except (KeyError, ValueError) as e:
                print(
                    f"Skipping ticker processing for {exchange_symbol} due to data error: {e}"
                )
                continue

        liquid_symbols_data.sort(
            key=lambda item: item["normalized_quote_volume"], reverse=True
        )
        return liquid_symbols_data[:top_n]

    async def close_session(self):
        # For BinanceSyncClient, no explicit async session to close.
        # If using BinanceAsyncClient, it would be: await self.async_client.close_connection()
        print(
            f"BinanceAdapter: No explicit async session to close for BinanceSyncClient."
        )
        pass
