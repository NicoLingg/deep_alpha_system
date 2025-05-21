from typing import Optional, Tuple
from decimal import Decimal, InvalidOperation

# Define standardized instrument type constants
SPOT = "SPOT"
PERP = "PERP"
FUTURE = "FUTURE"  # Generic prefix for dated futures


class SymbolRepresentation:
    def __init__(
        self,
        base_asset: str,
        quote_asset: str,
        instrument_type: str = SPOT,
        expiry_date: Optional[str] = None,  # Format: YYMMDD for futures
        strike_price: Optional[Decimal] = None,  # For options
        option_type: Optional[str] = None,  # 'CALL' or 'PUT' for options
    ):
        self.base_asset = base_asset.upper()
        self.quote_asset = quote_asset.upper()
        self.instrument_type = instrument_type.upper()
        self.expiry_date = expiry_date  # Assumed to be in YYMMDD format if provided
        self.strike_price = strike_price
        self.option_type = option_type.upper() if option_type else None

        if self.instrument_type == FUTURE and not self.expiry_date:
            raise ValueError("Expiry date is required for FUTURE instrument type.")
        if self.instrument_type == "OPTION" and (
            not self.expiry_date or not self.strike_price or not self.option_type
        ):
            raise ValueError(
                "Expiry date, strike price, and option type are required for OPTION instrument type."
            )

    @property
    def normalized(self) -> str:
        """Generates the standard internal string representation."""
        core = f"{self.base_asset}-{self.quote_asset}"
        if self.instrument_type == SPOT:
            return core
        elif self.instrument_type == PERP:
            return f"{core}-{PERP}"
        elif self.instrument_type == FUTURE and self.expiry_date:
            return f"{core}-{FUTURE}_{self.expiry_date}"  # e.g. BTC-USD-FUTURE_241231
        elif (
            self.instrument_type == "OPTION"
            and self.expiry_date
            and self.strike_price
            and self.option_type
        ):
            # Format: BASE-QUOTE-STRIKE-EXPIRY-OPTIONTYPE (e.g., BTC-USD-50000-241231-CALL)
            return (
                f"{core}-{str(self.strike_price)}-{self.expiry_date}-{self.option_type}"
            )
        # Fallback or error for unhandled types
        return (
            f"{core}-{self.instrument_type}"  # Generic if not SPOT/PERP/FUTURE/OPTION
        )

    @classmethod
    def parse(cls, standard_symbol_str: str) -> "SymbolRepresentation":
        """Parses a standard internal string representation into a SymbolRepresentation object."""
        parts = standard_symbol_str.upper().split("-")
        if len(parts) < 2:
            raise ValueError(
                f"Invalid standard symbol string: '{standard_symbol_str}'. Expected at least 'BASE-QUOTE'."
            )

        base = parts[0]
        quote = parts[1]
        instrument_type = SPOT
        expiry_date = None
        strike_price = None
        option_type = None

        if len(parts) == 3:  # e.g., BTC-USD-PERP or BTC-USD-FUTURE_YYMMDD
            type_part = parts[2]
            if type_part == PERP:
                instrument_type = PERP
            elif type_part.startswith(FUTURE) and "_" in type_part:
                instrument_type = FUTURE
                expiry_date = type_part.split("_")[1]
                if not (expiry_date.isdigit() and len(expiry_date) == 6):
                    raise ValueError(
                        f"Invalid future expiry date format in '{standard_symbol_str}'. Expected YYMMDD."
                    )
            else:  # Could be a simple type like BTC-USD-OPTION (though our normalized is more specific)
                # Or just an unrecognised type part. For now, assume it's the instrument type itself.
                instrument_type = type_part

        elif len(parts) == 5:  # Option: BASE-QUOTE-STRIKE-EXPIRY-OPTIONTYPE
            try:
                strike_price = Decimal(parts[2])
            except InvalidOperation:
                raise ValueError(
                    f"Invalid strike price in option symbol: '{standard_symbol_str}'"
                )

            expiry_part = parts[3]  # Should be YYMMDD
            if not (expiry_part.isdigit() and len(expiry_part) == 6):
                raise ValueError(
                    f"Invalid option expiry date format in '{standard_symbol_str}'. Expected YYMMDD."
                )
            expiry_date = expiry_part

            option_type_part = parts[4]
            if option_type_part not in ["CALL", "PUT"]:
                raise ValueError(
                    f"Invalid option type in symbol: '{standard_symbol_str}'. Expected CALL or PUT."
                )
            option_type = option_type_part
            instrument_type = "OPTION"

        elif (
            len(parts) > 2
        ):  # Unhandled more complex type, treat the last part as type for now
            instrument_type = parts[-1]

        return cls(base, quote, instrument_type, expiry_date, strike_price, option_type)

    def __repr__(self):
        return f"<SymbolRepresentation: {self.normalized}>"

    def __eq__(self, other):
        if not isinstance(other, SymbolRepresentation):
            return NotImplemented
        return self.normalized == other.normalized

    def __hash__(self):
        return hash(self.normalized)
