import os
import psycopg2
import configparser
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine
from typing import Optional, Tuple, Any

from .exchanges.base_interface import ExchangeInterface
from .exchanges.binance_adapter import BinanceAdapter

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.ini")

_db_engine_sqlalchemy = None
_db_conn_psycopg = None

_EXCHANGE_ADAPTER_MAP = {
    "binance": BinanceAdapter,
    # "coinbase": CoinbaseAdapter, # Example for another exchange
}


def load_config(config_path: Optional[str] = None) -> configparser.ConfigParser:
    path_to_load: str
    if config_path is None:
        path_to_load = DEFAULT_CONFIG_PATH
    elif os.path.isabs(config_path):
        path_to_load = config_path
    else:
        path_to_load = os.path.join(PROJECT_ROOT, config_path)

    if not os.path.exists(path_to_load):
        cwd_path = os.path.join(
            os.getcwd(),
            config_path if config_path else os.path.basename(DEFAULT_CONFIG_PATH),
        )
        if os.path.exists(cwd_path):
            path_to_load = cwd_path
        else:
            potential_cwd_config_path = os.path.join(
                os.getcwd(),
                "config",
                os.path.basename(config_path or DEFAULT_CONFIG_PATH),
            )
            if os.path.exists(potential_cwd_config_path):
                path_to_load = potential_cwd_config_path
            else:
                raise FileNotFoundError(
                    f"Config file not found. Checked: {path_to_load}, {cwd_path}, and {potential_cwd_config_path}"
                )

    parser = configparser.ConfigParser()
    try:
        parser.read(path_to_load)
        if not parser.has_section("database"):
            raise configparser.NoSectionError(
                f"database section not found in {path_to_load}"
            )
        required_db_keys = ["host", "port", "dbname", "user", "password"]
        for key in required_db_keys:
            if not parser.has_option("database", key):
                raise configparser.NoOptionError(key, "database", f"in {path_to_load}")
    except configparser.Error as e:
        print(f"Error reading or parsing config file '{path_to_load}': {e}")
        raise
    return parser


def get_exchange_adapter(
    exchange_name: str, config: configparser.ConfigParser
) -> ExchangeInterface:
    exchange_name_lower = exchange_name.lower()
    AdapterClass = _EXCHANGE_ADAPTER_MAP.get(exchange_name_lower)
    if not AdapterClass:
        raise ValueError(
            f"Unsupported exchange: '{exchange_name}'. Available: {list(_EXCHANGE_ADAPTER_MAP.keys())}"
        )

    adapter_config_overrides = {}
    if config.has_section(exchange_name_lower):
        adapter_config_overrides = dict(config.items(exchange_name_lower))

    api_key = adapter_config_overrides.pop("api_key", None)
    api_secret = adapter_config_overrides.pop("api_secret", None)

    try:
        return AdapterClass(
            api_key=api_key,
            api_secret=api_secret,
            config_override=adapter_config_overrides,
        )
    except Exception as e:
        raise ValueError(f"Error initializing adapter for {exchange_name}: {e}")


def get_sqlalchemy_engine(
    config_object: Optional[configparser.ConfigParser] = None,
    new_instance: bool = False,
):
    global _db_engine_sqlalchemy
    current_config = config_object if config_object else load_config()

    if new_instance or _db_engine_sqlalchemy is None:
        try:
            db_user = current_config["database"]["user"]
            db_password = current_config["database"]["password"]
            db_host = current_config["database"]["host"]
            db_port = current_config["database"]["port"]
            db_name = current_config["database"]["dbname"]
            db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(db_url, pool_pre_ping=True, echo=False)
            if not new_instance:
                _db_engine_sqlalchemy = engine
            return engine
        except KeyError as e:
            print(f"Error: Missing database configuration key for SQLAlchemy: {e}")
            raise
        except Exception as e:
            print(f"Error creating SQLAlchemy engine: {e}")
            raise
    return _db_engine_sqlalchemy


def get_db_connection(
    app_config: configparser.ConfigParser,
    new_instance: bool = False,
    cursor_factory=None,
):
    global _db_conn_psycopg

    if not new_instance and _db_conn_psycopg and not _db_conn_psycopg.closed:
        if cursor_factory and not isinstance(
            _db_conn_psycopg.cursor_factory, cursor_factory
        ):
            pass
        else:
            return _db_conn_psycopg

    try:
        db_params = {
            "host": app_config.get("database", "host"),
            "port": app_config.get("database", "port"),
            "dbname": app_config.get("database", "dbname"),
            "user": app_config.get("database", "user"),
            "password": app_config.get("database", "password"),
        }
        db_params["cursor_factory"] = cursor_factory if cursor_factory else DictCursor
        conn = psycopg2.connect(**db_params)
        if not new_instance:
            _db_conn_psycopg = conn
        return conn
    except (
        psycopg2.Error,
        KeyError,
        configparser.NoSectionError,
        configparser.NoOptionError,
    ) as e:
        print(f"Database connection error: {e}")
        return None


def get_or_create_exchange_id(cursor, exchange_name: str) -> int:
    exchange_name_lower = exchange_name.lower()
    cursor.execute(
        "SELECT exchange_id FROM exchanges WHERE name = %s", (exchange_name_lower,)
    )
    result_row = cursor.fetchone()
    if result_row:
        return result_row["exchange_id"]
    else:
        cursor.execute(
            "INSERT INTO exchanges (name) VALUES (%s) RETURNING exchange_id",
            (exchange_name_lower,),
        )
        new_exchange_id_row = cursor.fetchone()
        if new_exchange_id_row:
            print(
                f"Created new exchange '{exchange_name_lower}' with ID: {new_exchange_id_row['exchange_id']}"
            )
            return new_exchange_id_row["exchange_id"]
        else:
            raise Exception(
                f"Failed to create or retrieve exchange_id for {exchange_name_lower}"
            )


def get_or_create_symbol_id(
    cursor,
    exchange_id: int,
    exchange_instrument_name: str,
    standard_base_asset: str,
    standard_quote_asset: str,
    instrument_type: str = "SPOT",
) -> Tuple[int, bool]:
    exchange_instrument_name_db = exchange_instrument_name.upper()
    standard_base_asset_db = standard_base_asset.upper()
    standard_quote_asset_db = standard_quote_asset.upper()
    instrument_type_db = instrument_type.upper()

    cursor.execute(
        """
        SELECT symbol_id FROM symbols
        WHERE exchange_id = %s AND instrument_name = %s
        """,
        (exchange_id, exchange_instrument_name_db),
    )
    result_row = cursor.fetchone()
    if result_row:
        return result_row["symbol_id"], False
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
                symbol_id_val = symbol_id_val_row["symbol_id"]
                print(
                    f"Created new symbol: {exchange_instrument_name_db} (ExID: {exchange_id}) -> "
                    f"Std: {standard_base_asset_db}-{standard_quote_asset_db} ({instrument_type_db}), DB SymID: {symbol_id_val}"
                )
                return symbol_id_val, True
            else:
                raise Exception(
                    f"Failed to create symbol {exchange_instrument_name_db} after attempting insert."
                )
        except psycopg2.Error as e:
            print(f"Error inserting symbol {exchange_instrument_name_db}: {e}")
            raise
