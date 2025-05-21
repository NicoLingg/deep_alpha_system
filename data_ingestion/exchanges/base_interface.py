from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any, Optional
from decimal import Decimal


class ExchangeInterface(ABC):
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        config_override: Optional[Dict[str, Any]] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.config_override = config_override if config_override else {}

    @abstractmethod
    def get_exchange_name(self) -> str:
        """Returns the lowercase, unique, standardized name of the exchange (e.g., 'binance', 'coinbasepro')."""
        pass

    @abstractmethod
    async def fetch_klines(
        self,
        standard_symbol_str: str,  # Your internal standard symbol, e.g., "BTC-USDT" or "BTC-USD-PERP"
        interval: str,
        start_datetime: Optional[pd.Timestamp] = None,  # UTC
        end_datetime: Optional[pd.Timestamp] = None,  # UTC
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Fetches kline data for the given STANDARD symbol.
        Adapter is responsible for converting standard_symbol_str to exchange-specific format.
        Returns a DataFrame with standardized columns:
        - 'time': pd.Timestamp, UTC, kline open time.
        - 'open', 'high', 'low', 'close': Decimal.
        - 'volume': Decimal, base asset volume.
        - 'quote_volume': Decimal, quote asset volume.
        - 'close_timestamp': pd.Timestamp, UTC, kline close time (nullable).
        - 'trade_count': pd.Int64Dtype (to support NA), number of trades (nullable).
        - 'taker_base_volume': Decimal, taker buy base asset volume (nullable).
        - 'taker_quote_volume': Decimal, taker buy quote asset volume (nullable).
        DataFrame should be empty if no data, not None. Columns should be present even if all values are None/NA.
        """
        pass

    @abstractmethod
    async def fetch_orderbook_snapshot(
        self, standard_symbol_str: str, limit: int = 100
    ) -> Dict[str, Any]:
        """
        Fetches an order book snapshot for the given STANDARD symbol.
        Adapter converts standard_symbol_str to exchange-specific format.
        Returns a dict like:
        {
            'bids': [[Decimal(price_str), Decimal(size_str)], ...],
            'asks': [[Decimal(price_str), Decimal(size_str)], ...],
            'lastUpdateId': Any,
            'exchange_ts': Optional[pd.Timestamp]
        }
        Prices and sizes should be Decimal. Bids high to low, asks low to high.
        Returns empty lists for bids/asks if error or no data.
        """
        pass

    @abstractmethod
    async def fetch_exchange_symbols_info(self) -> List[Dict[str, Any]]:
        """
        Fetches information about all (or relevant) symbols available on the exchange.
        Adapter standardizes base_asset, quote_asset, instrument_type.
        Returns a list of dicts, each with keys:
        'exchange_specific_symbol', 'base_asset', 'quote_asset', 'instrument_type',
        'status', 'tick_size', 'lot_size', 'min_notional', 'raw_details'.
        """
        pass

    @abstractmethod
    def normalize_standard_symbol_to_exchange(self, standard_symbol_str: str) -> str:
        """
        Converts your internal standard symbol string to the exchange-specific format.
        Should primarily use an internal cache populated by fetch_exchange_symbols_info.
        """
        pass

    @abstractmethod
    def normalize_exchange_symbol_to_standard(
        self, exchange_specific_symbol: str, instrument_type_hint: Optional[str] = None
    ) -> str:
        """
        Converts an exchange-specific symbol string to your internal standard format.
        Should primarily use an internal cache populated by fetch_exchange_symbols_info.
        """
        pass

    @abstractmethod
    async def check_api_symbol_status(
        self, exchange_specific_symbol: str
    ) -> Dict[str, Any]:
        """
        Checks the live trading status of a specific symbol on the exchange.
        Returns a dict with 'status', 'is_trading_allowed', etc.
        """
        pass

    @abstractmethod
    async def get_top_liquid_symbols(
        self,
        top_n: int = 20,
        standard_quote_asset_filter: Optional[str] = "USDT",
        min_volume: float = 0,
    ) -> List[Dict[str, Any]]:
        """
        Fetches ticker data and returns top N liquid symbols.
        Returns list of dicts: {'exchange_specific_symbol', 'standard_symbol', 'normalized_quote_volume', 'raw_ticker_data'}
        """
        pass

    async def close_session(self):
        """Clean up any persistent resources, like HTTP client sessions."""
        print(
            f"Closing session for {self.get_exchange_name()} adapter (if applicable)."
        )
        pass
