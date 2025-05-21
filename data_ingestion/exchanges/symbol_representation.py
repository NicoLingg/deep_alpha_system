from typing import Optional, Tuple
from decimal import Decimal, InvalidOperation

# Define standardized instrument type constants
SPOT = "SPOT"
PERP = "PERP"
FUTURE = "FUTURE"
OPTION = "OPTION"


class SymbolRepresentation:
    def __init__(
        self,
        base_asset: str,
        quote_asset: str,
        instrument_type: str = SPOT,
        expiry_date: Optional[str] = None,  # Format: YYMMDD for futures/options
        strike_price: Optional[Decimal] = None,  # For options
        option_type: Optional[str] = None,  # 'CALL' or 'PUT' for options
    ):
        self.base_asset = base_asset.upper()
        self.quote_asset = quote_asset.upper()
        self.instrument_type = instrument_type.upper()
        self.expiry_date = expiry_date  # Assumed to be in YYMMDD format if provided
        self.strike_price = strike_price
        self.option_type = option_type.upper() if option_type else None

        # If instrument_type is passed as "FUTURE_YYMMDD", parse it
        if self.instrument_type.startswith(f"{FUTURE}_") and not self.expiry_date:
            parts = self.instrument_type.split("_", 1)
            if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 6:
                self.instrument_type = FUTURE  # Set base type
                self.expiry_date = parts[1]
            # Else, it remains as is, validation below might catch it or it's treated as a custom type

        if self.instrument_type == FUTURE and not self.expiry_date:
            raise ValueError("Expiry date is required for FUTURE instrument type.")
        if self.instrument_type == OPTION and (
            not self.expiry_date or not self.strike_price or not self.option_type
        ):
            raise ValueError(
                "Expiry date, strike price, and option type are required for OPTION instrument type."
            )
        if self.option_type and self.option_type not in ["CALL", "PUT"]:
            raise ValueError("Option type must be 'CALL' or 'PUT'.")

    @property
    def normalized(self) -> str:
        """Generates the standard internal string representation."""
        core = f"{self.base_asset}-{self.quote_asset}"
        if self.instrument_type == SPOT:
            return core
        elif self.instrument_type == PERP:
            return f"{core}-{PERP}"
        elif self.instrument_type == FUTURE and self.expiry_date:
            # Ensures YYMMDD format for expiry_date if it's a FUTURE type
            if not (
                isinstance(self.expiry_date, str)
                and self.expiry_date.isdigit()
                and len(self.expiry_date) == 6
            ):
                raise ValueError(
                    f"Invalid expiry date format for FUTURE: '{self.expiry_date}'. Expected YYMMDD."
                )
            return f"{core}-{FUTURE}_{self.expiry_date}"
        elif (
            self.instrument_type == OPTION
            and self.expiry_date
            and self.strike_price is not None  # strike_price can be 0
            and self.option_type
        ):
            if not (
                isinstance(self.expiry_date, str)
                and self.expiry_date.isdigit()
                and len(self.expiry_date) == 6
            ):
                raise ValueError(
                    f"Invalid expiry date format for OPTION: '{self.expiry_date}'. Expected YYMMDD."
                )
            return (
                f"{core}-{str(self.strike_price)}-{self.expiry_date}-{self.option_type}"
            )
        # Fallback for unhandled recognized types or if FUTURE/OPTION parts are missing
        # Or if instrument_type was something like "FUTURE_INVALIDDATE"
        if self.instrument_type in [
            FUTURE,
            OPTION,
        ]:  # If it's a base type but components missing
            raise ValueError(
                f"Components missing for instrument type {self.instrument_type} to be normalized. Expiry: {self.expiry_date}, Strike: {self.strike_price}, OptionType: {self.option_type}"
            )

        return (
            f"{core}-{self.instrument_type}"  # Generic for other types or if malformed
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
        instrument_type_str = SPOT  # Default
        expiry_date = None
        strike_price = None
        option_type = None

        if len(parts) == 2:  # BTC-USD -> SPOT
            instrument_type_str = SPOT
        elif len(parts) == 3:  # e.g., BTC-USD-PERP or BTC-USD-FUTURE_YYMMDD
            type_part = parts[2]
            if type_part == PERP:
                instrument_type_str = PERP
            elif type_part.startswith(f"{FUTURE}_"):
                future_parts = type_part.split("_", 1)
                if (
                    len(future_parts) == 2
                    and future_parts[0] == FUTURE
                    and future_parts[1].isdigit()
                    and len(future_parts[1]) == 6
                ):
                    instrument_type_str = FUTURE
                    expiry_date = future_parts[1]
                else:
                    raise ValueError(
                        f"Invalid future format: '{type_part}'. Expected 'FUTURE_YYMMDD'."
                    )
            else:  # Could be a simple type like BTC-USD-TYPE
                instrument_type_str = type_part

        elif len(parts) == 5:  # Option: BASE-QUOTE-STRIKE-EXPIRY-OPTIONTYPE
            # e.g. BTC-USD-50000-241231-CALL
            instrument_type_str = OPTION
            try:
                strike_price = Decimal(parts[2])
            except InvalidOperation:
                raise ValueError(
                    f"Invalid strike price '{parts[2]}' in option symbol: '{standard_symbol_str}'"
                )

            expiry_part = parts[3]
            if not (expiry_part.isdigit() and len(expiry_part) == 6):
                raise ValueError(
                    f"Invalid option expiry date format '{expiry_part}' in '{standard_symbol_str}'. Expected YYMMDD."
                )
            expiry_date = expiry_part

            option_type_part = parts[4]
            if option_type_part not in ["CALL", "PUT"]:
                raise ValueError(
                    f"Invalid option type '{option_type_part}' in symbol: '{standard_symbol_str}'. Expected CALL or PUT."
                )
            option_type = option_type_part

        elif (
            len(parts) > 2
        ):  # Catches 4 parts, or >5 parts, or 3 parts not matching PERP/FUTURE_
            # Treat as BASE-QUOTE-TYPE1-TYPE2... where entire suffix is instrument_type
            # For example, if symbol is "BTC-USD-MY-CUSTOM-TYPE"
            # This will result in instrument_type = "MY-CUSTOM-TYPE"
            # The .normalized property will then return "BTC-USD-MY-CUSTOM-TYPE"
            instrument_type_str = "-".join(parts[2:])

        return cls(
            base, quote, instrument_type_str, expiry_date, strike_price, option_type
        )

    def __repr__(self):
        try:
            norm = self.normalized
        except ValueError:  # If components are inconsistent for normalization
            norm = f"InconsistentState({self.base_asset}-{self.quote_asset}-{self.instrument_type})"
        return f"<SymbolRepresentation: {norm}>"

    def __eq__(self, other):
        if not isinstance(other, SymbolRepresentation):
            return NotImplemented
        try:
            return self.normalized == other.normalized
        except (
            ValueError
        ):  # If either cannot be normalized, they are not equal in a valid sense
            return False

    def __hash__(self):
        try:
            return hash(self.normalized)
        except ValueError:  # If not normalizable, hash a tuple of its parts
            return hash(
                (
                    self.base_asset,
                    self.quote_asset,
                    self.instrument_type,
                    self.expiry_date,
                    self.strike_price,
                    self.option_type,
                )
            )
