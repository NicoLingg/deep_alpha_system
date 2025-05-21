from typing import Optional, Dict, Any

# Standardized instrument type constants
SPOT = "SPOT"
PERP = "PERP"  # Perpetual Futures/Swaps
FUTURE = "FUTURE"  # Dated Futures


class SymbolRepresentation:
    """
    Represents a trading symbol with standardized components.
    - base_asset: The asset being traded (e.g., BTC).
    - quote_asset: The asset used for pricing (e.g., USDT, USD).
    - instrument_type: SPOT, PERP, or FUTURE.
    - expiry_date: For FUTURE type, in YYMMDD format (e.g., "241227").
    - exchange_specific_name: The symbol name as used by the exchange (e.g., "BTCUSDT", "BTC-PERP").
    """

    def __init__(
        self,
        base_asset: str,
        quote_asset: str,
        instrument_type: str = SPOT,
        expiry_date: Optional[str] = None,
        exchange_specific_name: Optional[str] = None,
    ):
        self.base_asset = base_asset.upper()
        self.quote_asset = quote_asset.upper()
        self.instrument_type = instrument_type.upper()
        self.exchange_specific_name = exchange_specific_name

        if self.instrument_type == FUTURE:
            if expiry_date and (len(expiry_date) != 6 or not expiry_date.isdigit()):
                raise ValueError(
                    f"Expiry date for FUTURE must be in YYMMDD format (e.g., '241227'), got '{expiry_date}'"
                )
            self.expiry_date = expiry_date
        elif expiry_date is not None:
            raise ValueError(
                f"Expiry date '{expiry_date}' should only be provided for instrument_type FUTURE, not {self.instrument_type}."
            )
        else:
            self.expiry_date = None

        if self.instrument_type not in [SPOT, PERP, FUTURE]:
            raise ValueError(
                f"Invalid instrument_type: '{self.instrument_type}'. Must be one of {SPOT}, {PERP}, {FUTURE}."
            )

    @property
    def normalized(self) -> str:
        """
        Returns a standardized string representation of the symbol.
        Examples:
        - BTC-USDT (for SPOT)
        - ETH-USD-PERP (for PERP)
        - ADA-USD-FUTURE-240927 (for dated FUTURE)
        - BTC-USDT-FUTURE (for undated/generic FUTURE, if expiry_date is None)
        """
        name = f"{self.base_asset}-{self.quote_asset}"
        if self.instrument_type != SPOT: # SPOT is implied if no type suffix
            name += f"-{self.instrument_type}"
            if self.instrument_type == FUTURE and self.expiry_date:
                name += f"-{self.expiry_date}"
        return name

    @classmethod
    def parse(cls, standard_symbol_str: str) -> "SymbolRepresentation":
        """
        Parses a standardized symbol string into a SymbolRepresentation object.
        Handles formats like:
        - "BTC-USDT" (implies SPOT)
        - "BTC-USDT-SPOT"
        - "ETH-USD-PERP"
        - "ADA-USD-FUTURE-240927"
        - "LINK-USDT-FUTURE" (generic future, expiry_date will be None)
        """
        parts = standard_symbol_str.strip().upper().split("-")
        if len(parts) < 2:
            raise ValueError(
                f"Invalid standard symbol string '{standard_symbol_str}'. Expected at least 'BASE-QUOTE'."
            )

        base = parts[0]
        quote = parts[1]
        instrument_type = SPOT
        expiry = None

        if len(parts) > 2:
            type_candidate = parts[2]
            if type_candidate in [SPOT, PERP, FUTURE]:
                instrument_type = type_candidate
                if instrument_type == FUTURE and len(parts) > 3:
                    # Check if the 4th part is a valid date (YYMMDD)
                    if len(parts[3]) == 6 and parts[3].isdigit():
                        expiry = parts[3]
                    # else: it's a generic FUTURE without a specific expiry in the string
            # If parts[2] is not SPOT, PERP, or FUTURE, it's likely part of a complex quote asset or an old format.
            # For simplicity here, we assume SPOT if parts[2] isn't a recognized type.
            # More complex parsing (e.g. "BTC-USDC.E-SPOT") would need specific logic.

        return cls(base, quote, instrument_type, expiry, standard_symbol_str)

    def __eq__(self, other):
        if not isinstance(other, SymbolRepresentation):
            return NotImplemented
        return (
            self.base_asset == other.base_asset
            and self.quote_asset == other.quote_asset
            and self.instrument_type == other.instrument_type
            and self.expiry_date == other.expiry_date
        )

    def __hash__(self):
        return hash(
            (
                self.base_asset,
                self.quote_asset,
                self.instrument_type,
                self.expiry_date,
            )
        )

    def __repr__(self):
        expiry_str = f", expiry='{self.expiry_date}'" if self.expiry_date else ""
        return (
            f"SymbolRepresentation(base='{self.base_asset}', quote='{self.quote_asset}', type='{self.instrument_type}'"
            f"{expiry_str}, exch_specific='{self.exchange_specific_name or 'N/A'}')"
        )

    @classmethod
    def from_api_dict(cls, api_symbol_info: Dict[str, Any]) -> "SymbolRepresentation":
        """
        Creates a SymbolRepresentation instance from a typical dictionary
        returned by an adapter's fetch_exchange_symbols_info() method.
        Assumes the dictionary contains standardized keys like 'base_asset',
        'quote_asset', 'instrument_type', and 'exchange_specific_symbol'.
        The 'instrument_type' can be like 'FUTURE_YYMMDD'.
        """
        base = api_symbol_info.get("base_asset")
        quote = api_symbol_info.get("quote_asset")
        instrument_type_raw = api_symbol_info.get("instrument_type", SPOT) # Default to SPOT if not present
        exchange_specific = api_symbol_info.get("exchange_specific_symbol")

        if not base or not quote:
            raise ValueError(f"Missing base_asset or quote_asset in API dict for symbol: {exchange_specific or api_symbol_info}")

        instrument_type_parsed = instrument_type_raw
        expiry_date_parsed = None

        if instrument_type_raw.startswith(f"{FUTURE}_"):
            parts = instrument_type_raw.split("_", 1)
            if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 6:
                instrument_type_parsed = FUTURE
                expiry_date_parsed = parts[1]
        
        return cls(base, quote, instrument_type_parsed, expiry_date_parsed, exchange_specific)