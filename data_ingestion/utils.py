import os
import psycopg2
import configparser
from psycopg2.extras import DictCursor  # Default cursor
from psycopg2.extensions import cursor as PgCursor  # For type hinting
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine as SqlAlchemyEngine  # For type hinting
from typing import Optional, Tuple, Any, Type, Dict
import logging
import logging.config
from .exchanges.base_interface import ExchangeInterface


# --- Project Root and Default Config Path ---
# Assuming utils.py is in data_ingestion, which is one level down from project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.ini")

logger = logging.getLogger(__name__)  # Logger for this module

# --- Global DB Objects (managed by getter functions) ---
_db_engine_sqlalchemy: Optional[SqlAlchemyEngine] = None
_db_conn_psycopg: Optional[psycopg2.extensions.connection] = None


# --- Logging Setup ---
def setup_logging(level=logging.INFO):
    """Basic logging configuration."""
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": level,  # Set level for console handler
                "stream": "ext://sys.stdout",  # Default is stderr
            }
        },
        "root": {  # Configure root logger
            "handlers": ["console"],
            "level": level,  # Set root logger level
        },
        # Example: Configure specific loggers if needed
        # 'loggers': {
        #     'data_ingestion.binance_adapter': {
        #         'handlers': ['console'],
        #         'level': 'DEBUG', # More verbose for specific module
        #         'propagate': False # Don't pass to root logger if handled here
        #     }
        # }
    }
    logging.config.dictConfig(logging_config)
    # logger.debug("Logging configured.") # Use after config is set


# --- Adapter Mapping (Late import to avoid circular dependencies if adapters use utils) ---
_EXCHANGE_ADAPTER_MAP: Optional[Dict[str, Any]] = None


def _get_adapter_map():
    global _EXCHANGE_ADAPTER_MAP
    if _EXCHANGE_ADAPTER_MAP is None:
        from .exchanges.binance_adapter import BinanceAdapter  # Example

        # from .exchanges.coinbase_adapter import CoinbaseAdapter # Example
        _EXCHANGE_ADAPTER_MAP = {
            "binance": BinanceAdapter,
            # "coinbase": CoinbaseAdapter,
        }
    return _EXCHANGE_ADAPTER_MAP


# --- Configuration Loading ---
def load_config(config_path: Optional[str] = None) -> configparser.ConfigParser:
    path_to_load: str
    if config_path is None:
        path_to_load = DEFAULT_CONFIG_PATH
    elif os.path.isabs(config_path):
        path_to_load = config_path
    else:  # Relative path, assume relative to project root
        path_to_load = os.path.join(PROJECT_ROOT, config_path)

    # Fallback checks if not found at primary location
    if not os.path.exists(path_to_load):
        # Check relative to CWD
        cwd_path = os.path.join(
            os.getcwd(),
            config_path if config_path else os.path.basename(DEFAULT_CONFIG_PATH),
        )
        if os.path.exists(cwd_path):
            path_to_load = cwd_path
        else:
            # Check CWD/config/ P(like project_root/config/)
            potential_cwd_config_path = os.path.join(
                os.getcwd(),
                "config",
                os.path.basename(config_path or DEFAULT_CONFIG_PATH),
            )
            if os.path.exists(potential_cwd_config_path):
                path_to_load = potential_cwd_config_path
            else:
                raise FileNotFoundError(
                    f"Config file not found. Checked: '{path_to_load}' (primary), '{cwd_path}' (cwd), and '{potential_cwd_config_path}' (cwd/config)."
                )

    logger.debug(f"Loading configuration from: {path_to_load}")
    parser = configparser.ConfigParser()
    try:
        parser.read(path_to_load)
        if not parser.has_section("database"):
            raise configparser.NoSectionError(
                f"'database' section not found in {path_to_load}"
            )

        required_db_keys = ["host", "port", "dbname", "user", "password"]
        missing_keys = [
            key for key in required_db_keys if not parser.has_option("database", key)
        ]
        if missing_keys:
            raise configparser.NoOptionError(
                f"Missing required database options: {', '.join(missing_keys)}",
                "database",
                f"in {path_to_load}",
            )

    except configparser.Error as e:
        logger.error(
            f"Error reading or parsing config file '{path_to_load}': {e}", exc_info=True
        )
        raise
    return parser


# --- Exchange Adapter Factory ---
def get_exchange_adapter(
    exchange_name: str, config: configparser.ConfigParser
) -> "ExchangeInterface":  # Forward ref
    from .exchanges.base_interface import (
        ExchangeInterface,
    )  # Local import for type hint

    adapter_map = _get_adapter_map()
    exchange_name_lower = exchange_name.lower()
    AdapterClass = adapter_map.get(exchange_name_lower)

    if not AdapterClass:
        raise ValueError(
            f"Unsupported exchange: '{exchange_name}'. Available: {list(adapter_map.keys())}"
        )

    adapter_config_overrides = {}
    if config.has_section(
        exchange_name_lower
    ):  # Section for exchange-specific config like 'binance'
        adapter_config_overrides = dict(config.items(exchange_name_lower))

    # API keys can be in exchange-specific section or a general 'api_keys' section (less common here)
    # Example: config['binance']['api_key']
    api_key = adapter_config_overrides.pop("api_key", None)
    api_secret = adapter_config_overrides.pop("api_secret", None)

    # Fallback to environment variables if not in config (optional, good practice)
    if api_key is None:
        api_key = os.environ.get(f"{exchange_name_lower.upper()}_API_KEY")
    if api_secret is None:
        api_secret = os.environ.get(f"{exchange_name_lower.upper()}_API_SECRET")

    try:
        return AdapterClass(
            api_key=api_key,
            api_secret=api_secret,
            config_override=adapter_config_overrides,
        )
    except Exception as e:
        logger.error(
            f"Error initializing adapter for {exchange_name}: {e}", exc_info=True
        )
        raise ValueError(f"Error initializing adapter for {exchange_name}: {e}")


# --- Database Connection Getters ---
def get_sqlalchemy_engine(
    config_object: Optional[configparser.ConfigParser] = None,
    new_instance: bool = False,
) -> SqlAlchemyEngine:
    global _db_engine_sqlalchemy
    current_config = config_object if config_object else load_config()

    if (
        new_instance
        or _db_engine_sqlalchemy is None
        or _db_engine_sqlalchemy.dialect.is_disposed
    ):
        try:
            db_user = current_config["database"]["user"]
            db_password = current_config["database"]["password"]
            db_host = current_config["database"]["host"]
            db_port = current_config["database"]["port"]
            db_name = current_config["database"]["dbname"]
            db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

            # Consider pool settings for production
            engine = create_engine(
                db_url, pool_pre_ping=True, echo=False, pool_size=5, max_overflow=10
            )

            if not new_instance:
                _db_engine_sqlalchemy = engine
            return engine
        except KeyError as e:
            logger.error(
                f"Missing database configuration key for SQLAlchemy: {e}", exc_info=True
            )
            raise
        except Exception as e:
            logger.error(f"Error creating SQLAlchemy engine: {e}", exc_info=True)
            raise
    return _db_engine_sqlalchemy


def get_db_connection(
    app_config: configparser.ConfigParser,
    new_instance: bool = False,
    cursor_factory: Optional[Type[PgCursor]] = None,  # Allow specifying cursor factory
) -> psycopg2.extensions.connection:
    global _db_conn_psycopg

    if not new_instance and _db_conn_psycopg and not _db_conn_psycopg.closed:
        # Check if existing connection's cursor_factory matches desired, if specified
        # psycopg2 connections don't directly expose their cursor_factory after creation easily.
        # For simplicity, if new_instance is False and a connection exists, we return it.
        # If specific cursor_factory is critical and differs, new_instance=True should be used.
        return _db_conn_psycopg

    try:
        db_params = {
            "host": app_config.get("database", "host"),
            "port": app_config.get("database", "port"),
            "dbname": app_config.get("database", "dbname"),
            "user": app_config.get("database", "user"),
            "password": app_config.get("database", "password"),
        }
        # Use provided cursor_factory or default to DictCursor
        db_params["cursor_factory"] = cursor_factory if cursor_factory else DictCursor

        conn = psycopg2.connect(**db_params)
        if not new_instance:
            if (
                _db_conn_psycopg and not _db_conn_psycopg.closed
            ):  # Close old shared conn if creating new shared one
                _db_conn_psycopg.close()
            _db_conn_psycopg = conn
        return conn
    except (
        psycopg2.Error,
        KeyError,
        configparser.NoSectionError,
        configparser.NoOptionError,
    ) as e:
        logger.error(f"Database connection error: {e}", exc_info=True)
        # Return None or raise? Raising might be better for critical connections.
        # For now, matching original behavior of returning None.
        return None  # type: ignore


# --- DB Helper Functions for IDs ---
def get_or_create_exchange_id(cursor: PgCursor, exchange_name: str) -> int:
    exchange_name_lower = exchange_name.lower()
    cursor.execute(
        "SELECT exchange_id FROM exchanges WHERE name = %s", (exchange_name_lower,)
    )
    result_row = cursor.fetchone()
    if result_row:
        return (
            result_row[0]
            if isinstance(result_row, tuple)
            else result_row["exchange_id"]
        )
    else:
        cursor.execute(
            "INSERT INTO exchanges (name) VALUES (%s) RETURNING exchange_id",
            (exchange_name_lower,),
        )
        new_exchange_id_row = cursor.fetchone()
        if new_exchange_id_row:
            new_id = (
                new_exchange_id_row[0]
                if isinstance(new_exchange_id_row, tuple)
                else new_exchange_id_row["exchange_id"]
            )
            logger.info(
                f"Created new exchange '{exchange_name_lower}' with ID: {new_id}"
            )
            return new_id
        else:  # Should not happen if RETURNING is used and insert succeeds
            raise Exception(
                f"Failed to create or retrieve exchange_id for {exchange_name_lower} after insert attempt."
            )


def get_or_create_symbol_id(
    cursor: PgCursor,
    exchange_id: int,
    exchange_instrument_name: str,  # Exchange-specific e.g. BTCUSDT
    standard_base_asset: str,  # Standard e.g. BTC
    standard_quote_asset: str,  # Standard e.g. USDT
    instrument_type: str = "SPOT",  # Standard type e.g. SPOT, PERP, FUTURE_YYMMDD
) -> Tuple[int, bool]:  # (symbol_id, created_boolean)

    exchange_instrument_name_db = exchange_instrument_name.upper()
    standard_base_asset_db = standard_base_asset.upper()
    standard_quote_asset_db = standard_quote_asset.upper()
    instrument_type_db = instrument_type.upper()

    cursor.execute(
        "SELECT symbol_id FROM symbols WHERE exchange_id = %s AND instrument_name = %s",
        (exchange_id, exchange_instrument_name_db),
    )
    result_row = cursor.fetchone()
    if result_row:
        symbol_id = (
            result_row[0] if isinstance(result_row, tuple) else result_row["symbol_id"]
        )
        # Optionally, verify other fields match or update them
        # For now, if (exchange_id, instrument_name) matches, assume it's the same symbol.
        return symbol_id, False  # False means already existed
    else:
        try:
            cursor.execute(
                """
                INSERT INTO symbols (exchange_id, instrument_name, base_asset, quote_asset, instrument_type)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING symbol_id
                """,
                (
                    exchange_id,
                    exchange_instrument_name_db,
                    standard_base_asset_db,
                    standard_quote_asset_db,
                    instrument_type_db,
                ),
            )
            symbol_id_val_row = cursor.fetchone()
            if symbol_id_val_row:
                symbol_id_val = (
                    symbol_id_val_row[0]
                    if isinstance(symbol_id_val_row, tuple)
                    else symbol_id_val_row["symbol_id"]
                )
                logger.info(
                    f"Created new symbol: {exchange_instrument_name_db} (ExID: {exchange_id}) -> "
                    f"StdBase: {standard_base_asset_db}, StdQuote: {standard_quote_asset_db}, Type: {instrument_type_db}. DB SymID: {symbol_id_val}"
                )
                return symbol_id_val, True  # True means newly created
            else:  # Should not happen
                raise Exception(
                    f"Failed to create symbol {exchange_instrument_name_db} (ExID: {exchange_id}) after attempting insert."
                )
        except (
            psycopg2.Error
        ) as e:  # Catch DB errors like unique constraint violation if somehow raced
            logger.error(
                f"Error inserting symbol {exchange_instrument_name_db} (ExID: {exchange_id}): {e}",
                exc_info=True,
            )
            raise  # Re-raise after logging
