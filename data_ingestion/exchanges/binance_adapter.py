import asyncio
import logging
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional
from binance.client import Client as BinanceSyncClient
from binance.exceptions import BinanceAPIException, BinanceRequestException

from .base_interface import ExchangeInterface
from .symbol_representation import SymbolRepresentation, SPOT, PERP, FUTURE

logger = logging.getLogger(__name__)

# Idea: Standardized internal quote asset concepts mapped FROM Binance's typical quote assets.
# For now we are NOT using this mapping, but it's here for future reference.
BINANCE_QUOTE_TO_STANDARD_MAP = {
    "USDT": "USDT",
    "BUSD": "BUSD",
    "USDC": "USDC",
    "TUSD": "TUSD",
    "PAX": "PAX",
    "USDP": "USDP",
    "DAI": "DAI",
    "BTC": "BTC",
    "ETH": "ETH",
}


STANDARD_QUOTE_TO_BINANCE_PERP_QUOTE_MAP = {
    "USD": "USD",
    "USDT": "USDT",
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
        self._exchange_info_list: Optional[List[Dict[str, Any]]] = None
        self._symbol_details_cache: Dict[str, Dict[str, Any]] = {}
        self._ensure_cache_task: Optional[asyncio.Task] = None
        self._cache_lock = asyncio.Lock()

    async def _ensure_cache_populated(self, force_refresh: bool = False):
        async with self._cache_lock:
            if self._ensure_cache_task and not self._ensure_cache_task.done():
                await self._ensure_cache_task
                return

            if self._exchange_info_list is None or force_refresh:
                self._ensure_cache_task = asyncio.create_task(
                    self._refresh_exchange_info_cache_impl()
                )
            else:
                return

        try:
            if self._ensure_cache_task:
                await self._ensure_cache_task
        finally:
            async with self._cache_lock:
                self._ensure_cache_task = None

    async def _refresh_exchange_info_cache_impl(self):
        logger.info(
            f"[{self.get_exchange_name().capitalize()}] Fetching/Refreshing exchange symbols info cache..."
        )
        try:
            raw_exchange_info = await asyncio.to_thread(
                self.sync_client.get_exchange_info
            )
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(
                f"Binance API error fetching exchange info: {e}", exc_info=True
            )
            self._exchange_info_list = []
            self._symbol_details_cache = {}
            return
        except Exception as e_generic:
            logger.error(
                f"Generic error fetching Binance exchange info: {e_generic}",
                exc_info=True,
            )
            self._exchange_info_list = []
            self._symbol_details_cache = {}
            return

        symbols_data_list = []
        temp_symbol_details_cache = {}

        for s_info in raw_exchange_info.get("symbols", []):
            exchange_specific_symbol = s_info["symbol"]
            binance_base_asset = s_info["baseAsset"]
            binance_quote_asset = s_info["quoteAsset"]

            standard_base_asset = binance_base_asset.strip().upper()
            # Use the map, fallback to the original quote asset if not in map
            standard_quote_asset = BINANCE_QUOTE_TO_STANDARD_MAP.get(
                binance_quote_asset.strip().upper(), binance_quote_asset.strip().upper()
            )

            instrument_type = SPOT
            expiry_date_str = None

            if s_info.get("contractType") == "PERPETUAL":
                instrument_type = PERP

            elif s_info.get("contractType") and s_info.get("contractType") not in [
                "NONE",
                "SPOT",
            ]:
                # COIN-M: BTCUSD_YYMMDD (quote asset in symbol is USD, margin is BTC)
                # USDT-M: BTCUSDTYYMMDD (quote asset in symbol is USDT, margin is USDT)
                if (
                    "_" in exchange_specific_symbol
                ):  # Typically COIN-M like BTCUSD_YYMMDD
                    parts = exchange_specific_symbol.split("_")
                    if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 6:
                        expiry_date_str = parts[-1]
                        instrument_type = f"{FUTURE}_{expiry_date_str}"
                        # For COIN-M like BTCUSD_YYMMDD, the conceptual quote is USD.
                        # standard_quote_asset should reflect this. Binance might return quoteAsset as 'USD'.
                        # If Binance returns baseAsset='BTC', quoteAsset='USD' for BTCUSD_241231, map handles it.
                elif (
                    len(exchange_specific_symbol) > 6
                    and exchange_specific_symbol[-6:].isdigit()
                ):
                    # Attempt to parse USDT-M futures like BTCUSDTYYMMDD
                    # Check if symbol starts with base+quote and ends with 6 digits
                    potential_base_quote = (
                        standard_base_asset + standard_quote_asset
                    )  # e.g. BTCUSDT
                    if (
                        exchange_specific_symbol.upper().startswith(
                            potential_base_quote
                        )
                        and len(exchange_specific_symbol)
                        == len(potential_base_quote) + 6
                    ):
                        expiry_date_str = exchange_specific_symbol[-6:]
                        instrument_type = f"{FUTURE}_{expiry_date_str}"
                else:
                    instrument_type = (
                        FUTURE  # Generic future if specific format not matched
                    )

            tick_size = Decimal("0")
            lot_size = Decimal("0")
            min_notional = Decimal("0")

            for f_filter in s_info.get("filters", []):
                if f_filter["filterType"] == "PRICE_FILTER":
                    tick_size = Decimal(str(f_filter["tickSize"]))
                elif f_filter["filterType"] == "LOT_SIZE":
                    lot_size = Decimal(str(f_filter["stepSize"]))
                elif f_filter["filterType"] == "MIN_NOTIONAL":
                    min_notional = Decimal(str(f_filter.get("notional", "0")))
                elif f_filter["filterType"] == "MARKET_LOT_SIZE":  # For some futures
                    pass  # lot_size already covered by LOT_SIZE for primary use
                elif (
                    f_filter["filterType"] == "NOTIONAL"
                ):  # Spot (older name for MIN_NOTIONAL)
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
        logger.info(
            f"[{self.get_exchange_name().capitalize()}] Cache populated with {len(self._exchange_info_list)} symbols."
        )

    def get_exchange_name(self) -> str:
        return "binance"

    def normalize_standard_symbol_to_exchange(self, standard_symbol_str: str) -> str:
        cleaned_standard_symbol_str = standard_symbol_str.strip()
        if not self._symbol_details_cache:
            logger.warning(
                f"Binance _symbol_details_cache is empty during normalize_standard_symbol_to_exchange for '{cleaned_standard_symbol_str}'. "
                "Attempting direct conversion fallback. Populate cache first for reliability."
            )
            try:
                s_repr = SymbolRepresentation.parse(cleaned_standard_symbol_str)
                if s_repr.instrument_type == SPOT:
                    return f"{s_repr.base_asset}{s_repr.quote_asset}"
                elif s_repr.instrument_type == PERP:
                    # Fallback uses STANDARD_QUOTE_TO_BINANCE_PERP_QUOTE_MAP for margin asset
                    binance_margin_asset = STANDARD_QUOTE_TO_BINANCE_PERP_QUOTE_MAP.get(
                        s_repr.quote_asset,
                        s_repr.quote_asset,
                    )
                    return f"{s_repr.base_asset}{binance_margin_asset}"  # e.g. BTCUSDT for BTC-USD-PERP (if USD->USDT map) or BTC-USDT-PERP
                elif s_repr.instrument_type == FUTURE and s_repr.expiry_date:
                    # COIN-M: BTCUSD_YYMMDD (standard: BTC-USD-FUTURE_YYMMDD)
                    # USDT-M: BTCUSDTYYMMDD (standard: BTC-USDT-FUTURE_YYMMDD)
                    if s_repr.quote_asset == "USD":  # Assume COIN-M like BTCUSD_YYMMDD
                        return f"{s_repr.base_asset}USD_{s_repr.expiry_date}"  # Explicitly USD for COIN-M
                    else:  # Assume USDT-M like BTCUSDTYYMMDD
                        return f"{s_repr.base_asset}{s_repr.quote_asset}{s_repr.expiry_date}"
                else:
                    raise ValueError(
                        f"Unsupported type for fallback normalization: '{s_repr.instrument_type}'"
                    )
            except ValueError as e:
                raise ValueError(
                    f"BinanceAdapter: Error parsing standard symbol '{cleaned_standard_symbol_str}' for fallback exchange normalization: {e}."
                )

        try:
            s_repr_lookup = SymbolRepresentation.parse(cleaned_standard_symbol_str)
        except ValueError as e:
            raise ValueError(
                f"BinanceAdapter: Invalid standard symbol string '{cleaned_standard_symbol_str}': {e}"
            )

        for ex_sym, details in self._symbol_details_cache.items():
            cached_instrument_type_base = details["instrument_type"]
            cached_expiry_date = None
            if details["instrument_type"].startswith(f"{FUTURE}_"):
                parts = details["instrument_type"].split("_", 1)
                if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 6:
                    cached_instrument_type_base = FUTURE
                    cached_expiry_date = parts[1]

            if (
                details["base_asset"] == s_repr_lookup.base_asset
                and details["quote_asset"] == s_repr_lookup.quote_asset
                and cached_instrument_type_base == s_repr_lookup.instrument_type
            ):
                if s_repr_lookup.instrument_type == FUTURE:
                    if cached_expiry_date == s_repr_lookup.expiry_date:
                        return ex_sym  # Return Binance's exchange_specific_symbol
                else:  # SPOT, PERP
                    return ex_sym

        logger.warning(
            f"Standard symbol '{cleaned_standard_symbol_str}' not found in cache by component match. Re-attempting direct conversion as final fallback."
        )
        try:  # Re-attempt direct conversion (same logic as initial fallback)
            s_repr_fallback = SymbolRepresentation.parse(cleaned_standard_symbol_str)
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
                if s_repr_fallback.quote_asset == "USD":
                    return (
                        f"{s_repr_fallback.base_asset}USD_{s_repr_fallback.expiry_date}"
                    )
                else:
                    return f"{s_repr_fallback.base_asset}{s_repr_fallback.quote_asset}{s_repr_fallback.expiry_date}"
        except Exception as e_fallback_direct:
            raise ValueError(
                f"BinanceAdapter: Unable to normalize standard symbol '{cleaned_standard_symbol_str}' to exchange format even with fallback. Error: {e_fallback_direct}. Consider refreshing cache."
            )

        raise ValueError(
            f"BinanceAdapter: Could not find exchange symbol for standard '{cleaned_standard_symbol_str}' in cache and all fallbacks failed."
        )

    async def normalize_exchange_symbol_to_standard(
        self, exchange_specific_symbol: str, instrument_type_hint: Optional[str] = None
    ) -> str:
        cleaned_exchange_symbol = exchange_specific_symbol.strip().upper()
        await self._ensure_cache_populated()

        details = self._symbol_details_cache.get(cleaned_exchange_symbol)
        if not details:
            logger.warning(
                f"BinanceAdapter: Exchange symbol '{cleaned_exchange_symbol}' not in cache. Refreshing cache once."
            )
            await self._ensure_cache_populated(force_refresh=True)
            details = self._symbol_details_cache.get(cleaned_exchange_symbol)
            if not details:
                raise ValueError(
                    f"BinanceAdapter: Exchange symbol '{cleaned_exchange_symbol}' not found in cache even after refresh. "
                    f"Hint: '{instrument_type_hint}'."
                )

        raw_instrument_type = details["instrument_type"]
        base_instrument_type_for_srepr = raw_instrument_type
        expiry_date_for_srepr = None

        if raw_instrument_type.startswith(f"{FUTURE}_"):
            parts = raw_instrument_type.split("_", 1)
            if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 6:
                base_instrument_type_for_srepr = FUTURE
                expiry_date_for_srepr = parts[1]

        s_repr = SymbolRepresentation(
            base_asset=details["base_asset"],
            quote_asset=details["quote_asset"],
            instrument_type=base_instrument_type_for_srepr,
            expiry_date=expiry_date_for_srepr,
        )
        return s_repr.normalized

    async def fetch_exchange_symbols_info(self) -> List[Dict[str, Any]]:
        await self._ensure_cache_populated(force_refresh=True)
        return self._exchange_info_list if self._exchange_info_list is not None else []

    async def fetch_klines(
        self,
        standard_symbol_str: str,
        interval: str,
        start_datetime: Optional[pd.Timestamp] = None,
        end_datetime: Optional[pd.Timestamp] = None,
        limit: Optional[int] = 1000,
    ) -> pd.DataFrame:
        await self._ensure_cache_populated()
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
        effective_limit = (
            limit if limit is not None and limit > 0 else 1000
        )  # Default to 1000 if None or invalid - Binance's max limit

        try:
            klines_data = await asyncio.to_thread(
                self.sync_client.get_historical_klines,
                exchange_symbol,
                interval,
                start_str=api_start_str,
                end_str=api_end_str,
                limit=effective_limit,
            )
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(
                f"Binance API error fetching klines for {exchange_symbol} (Standard: {standard_symbol_str}, Interval: {interval}): {e}",
                exc_info=True,
            )
            return pd.DataFrame()
        except Exception as e_generic:
            logger.error(
                f"Generic error fetching klines for {exchange_symbol} (Standard: {standard_symbol_str}, Interval: {interval}): {e_generic}",
                exc_info=True,
            )
            return pd.DataFrame()

        if not klines_data:
            return pd.DataFrame()

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

        df["time"] = pd.to_datetime(df["kline_open_time"], unit="ms", utc=True)
        df["close_timestamp"] = pd.to_datetime(
            df["kline_close_time"], unit="ms", utc=True
        )

        df.rename(
            columns={
                "quote_asset_volume": "quote_volume",
                "number_of_trades": "trade_count",
                "taker_buy_base_asset_volume": "taker_base_volume",
                "taker_buy_quote_asset_volume": "taker_quote_volume",
            },
            inplace=True,
        )

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
                ]:
                    df[col_name] = pd.Series([None] * len(df), dtype=object)
                else:
                    df[col_name] = pd.NA

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
                df[col] = (
                    df[col]
                    .astype(str)
                    .apply(  # astype(str) first for robust Decimal conversion
                        lambda x: (
                            Decimal(x)
                            if x not in ["None", "nan", "NaT", str(pd.NA), None, ""]
                            else None
                        )
                    )
                )
            else:
                df[col] = pd.Series([None] * len(df), dtype=object)

        if "trade_count" in df.columns:
            df["trade_count"] = pd.to_numeric(
                df["trade_count"], errors="coerce"
            ).astype(pd.Int64Dtype())
        else:
            df["trade_count"] = pd.Series([pd.NA] * len(df), dtype=pd.Int64Dtype())

        return df[standard_columns_output_order]

    async def fetch_orderbook_snapshot(
        self, standard_symbol_str: str, limit: int = 100
    ) -> Dict[str, Any]:
        await self._ensure_cache_populated()
        exchange_symbol = self.normalize_standard_symbol_to_exchange(
            standard_symbol_str
        )

        valid_limits = [5, 10, 20, 50, 100, 500, 1000, 5000]
        actual_limit = limit
        if limit not in valid_limits:
            actual_limit = next(
                (l for l in valid_limits if l >= limit), valid_limits[-1]
            )

        depth_response_raw: Optional[Dict[str, Any]] = None
        try:
            depth_response_raw = await asyncio.to_thread(
                self.sync_client.get_order_book,
                symbol=exchange_symbol,
                limit=actual_limit,
            )
            logger.debug(
                f"Raw order book depth response for {exchange_symbol} (limit {actual_limit}): {str(depth_response_raw)[:500]}"
            )
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(
                f"Binance API error fetching orderbook for {exchange_symbol} (Std: {standard_symbol_str}): {e}",
                exc_info=True,
            )
            return {"bids": [], "asks": [], "lastUpdateId": None, "exchange_ts": None}
        except Exception as e_generic:
            logger.error(
                f"Generic error fetching orderbook for {exchange_symbol} (Std: {standard_symbol_str}): {e_generic}",
                exc_info=True,
            )
            return {"bids": [], "asks": [], "lastUpdateId": None, "exchange_ts": None}

        # Spot order book snapshot from /api/v3/depth DOES NOT include 'E' or 'T'.
        # Futures order book snapshot from /fapi/v1/depth or /dapi/v1/depth DOES include 'E' and 'T'.
        # This adapter primarily uses client.get_order_book which hits /api/v3/depth for spot.
        # If futures order books are needed, a different client method (e.g., client.futures_order_book)
        # and corresponding parsing would be required.
        exchange_timestamp_ms = depth_response_raw.get(
            "T", depth_response_raw.get("E")
        )  # Will be None for SPOT
        exchange_pd_ts: Optional[pd.Timestamp] = None
        if exchange_timestamp_ms is not None:
            try:
                exchange_pd_ts = pd.to_datetime(
                    exchange_timestamp_ms, unit="ms", utc=True
                )
            except Exception as e_ts:
                logger.warning(
                    f"Could not parse exchange_timestamp_ms '{exchange_timestamp_ms}' for {exchange_symbol}: {e_ts}"
                )

        last_update_id_val = depth_response_raw.get("lastUpdateId")
        if last_update_id_val is None:
            logger.warning(
                f"lastUpdateId is missing from depth response for {exchange_symbol}. Keys: {depth_response_raw.keys()}"
            )
            # For SPOT, lastUpdateId should always be there. If it's missing, the response is unusual.

        return {
            "bids": [
                [Decimal(str(b[0])), Decimal(str(b[1]))]
                for b in depth_response_raw.get("bids", [])
            ],
            "asks": [
                [Decimal(str(a[0])), Decimal(str(a[1]))]
                for a in depth_response_raw.get("asks", [])
            ],
            "lastUpdateId": last_update_id_val,
            "exchange_ts": exchange_pd_ts,  # Will be None for SPOT markets
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
                    "status": "UNKNOWN_NO_INFO",
                    "is_trading_allowed": False,
                    "error_message": "No symbol info from API.",
                    "raw_details": None,
                }

            api_status = s_info.get("status", "UNKNOWN").upper()
            is_trading_allowed = api_status == "TRADING"  # Base check

            # Specific checks based on instrument type implied by response structure
            if "isSpotTradingAllowed" in s_info:  # Likely a SPOT symbol
                is_trading_allowed = is_trading_allowed and s_info.get(
                    "isSpotTradingAllowed", False
                )
            elif "contractType" in s_info:  # Likely a FUTURES/PERP symbol
                contract_status = s_info.get("contractStatus", "").upper()
                # For futures, TRADING status for the symbol itself and contractStatus=TRADING is needed
                if contract_status and contract_status != "TRADING":
                    is_trading_allowed = False

            return {
                "status": api_status,
                "is_trading_allowed": is_trading_allowed,
                "raw_details": s_info,
            }
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.warning(
                f"Binance API error checking status for {exchange_specific_symbol}: Code {e.code}, Msg: {e.message}"
            )
            status_on_error = "ERROR_API"
            if e.code == -1121:
                status_on_error = "UNKNOWN_INVALID_SYMBOL"  # Invalid symbol
            elif e.code == -1021:
                status_on_error = "ERROR_TS_SYNC"  # Timestamp for this request is outside of the recvWindow
            return {
                "status": status_on_error,
                "is_trading_allowed": False,
                "error_message": str(e),
                "error_code": e.code,
                "raw_details": None,
            }
        except Exception as e_generic:
            logger.error(
                f"Generic error checking API status for {exchange_specific_symbol}: {e_generic}",
                exc_info=True,
            )
            return {
                "status": "ERROR_GENERIC",
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
        await self._ensure_cache_populated()

        try:
            # client.get_ticker() fetches SPOT tickers.
            # For futures, client.futures_ticker() or client.delivery_ticker() would be needed.
            # This implementation currently targets SPOT liquidity.
            tickers_raw = await asyncio.to_thread(self.sync_client.get_ticker)
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(f"Binance API error fetching tickers: {e}", exc_info=True)
            return []
        except Exception as e_generic:
            logger.error(
                f"Generic error fetching tickers from Binance: {e_generic}",
                exc_info=True,
            )
            return []

        liquid_symbols_data = []
        for ticker_data in tickers_raw:
            exchange_symbol = ticker_data.get("symbol")
            if not exchange_symbol:
                continue

            cached_details = self._symbol_details_cache.get(exchange_symbol.upper())
            if not cached_details:
                # logger.debug(f"Skipping ticker {exchange_symbol}: Not found in cached exchange info.")
                continue

            # Filter: Only consider SPOT symbols if get_ticker() is for SPOT
            if cached_details["instrument_type"] != SPOT:
                continue

            if standard_quote_asset_filter:
                if (
                    cached_details["quote_asset"].upper()
                    != standard_quote_asset_filter.upper()
                ):
                    continue
            try:
                quote_volume_str = ticker_data.get("quoteVolume")
                if quote_volume_str is None:
                    continue
                quote_volume_val = float(quote_volume_str)

                if quote_volume_val < min_volume:
                    continue

                # Standard symbol string construction (already done in normalize_exchange_symbol_to_standard)
                # but we can reconstruct here for clarity or use the cached details directly
                s_repr = SymbolRepresentation(
                    base_asset=cached_details["base_asset"],
                    quote_asset=cached_details["quote_asset"],
                    instrument_type=SPOT,
                )
                standard_symbol_str_normalized = s_repr.normalized

                liquid_symbols_data.append(
                    {
                        "exchange_specific_symbol": exchange_symbol,
                        "standard_symbol": standard_symbol_str_normalized,
                        "normalized_quote_volume": quote_volume_val,
                        "raw_ticker_data": ticker_data,
                    }
                )
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(
                    f"Skipping ticker processing for {exchange_symbol} due to data error: {e}",
                    exc_info=False,
                )
                continue

        liquid_symbols_data.sort(
            key=lambda item: item["normalized_quote_volume"], reverse=True
        )
        return liquid_symbols_data[:top_n]

    async def close_session(self):
        logger.info(
            f"BinanceAdapter ({self.get_exchange_name()}): No explicit async session to close for BinanceSyncClient."
        )
        pass
