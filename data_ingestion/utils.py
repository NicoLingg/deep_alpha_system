import os
import psycopg2
import configparser
from binance.client import Client
from psycopg2.extras import DictCursor, RealDictCursor
import json
from sqlalchemy import create_engine

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.ini")

_db_engine = None  # Global SQLAlchemy engine for the ingestion part if needed later
_raw_db_conn_cache = (
    {}
)  # Cache for raw psycopg2 connections per thread/process if needed

_db_conn = None  # If you use a global connection pattern


def load_config(config_path=None):
    """
    Loads the configuration from the specified path or the default path.
    """
    if config_path is None:
        path_to_load = DEFAULT_CONFIG_PATH
    elif os.path.isabs(config_path):
        path_to_load = config_path
    else:
        path_to_load = os.path.join(PROJECT_ROOT, config_path)

    if not os.path.exists(path_to_load):
        cwd_path = os.path.join(
            os.getcwd(),
            config_path if config_path else os.path.join("config", "config.ini"),
        )
        if os.path.exists(cwd_path):
            path_to_load = cwd_path
        else:
            raise FileNotFoundError(
                f"Config file not found. Checked: {path_to_load} and {cwd_path}"
            )

    parser = configparser.ConfigParser()
    parser.read(path_to_load)
    return parser


def get_sqlalchemy_engine(config_object=None, new_instance=False):
    """
    Creates or returns a SQLAlchemy engine.
    If new_instance is True, it always creates a new engine (useful for threads/processes).
    """
    global _db_engine

    current_config = config_object if config_object else load_config()

    if new_instance or _db_engine is None:
        try:
            db_user = current_config["database"]["user"]
            db_password = current_config["database"]["password"]
            db_host = current_config["database"]["host"]
            db_port = current_config["database"]["port"]
            db_name = current_config["database"]["dbname"]

            db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(
                db_url, pool_pre_ping=True
            )  # Add pool_pre_ping for robustness

            if not new_instance:  # Store as global default if not requesting new
                _db_engine = engine
            return engine
        except KeyError as e:
            print(
                f"Error: Missing database configuration key: {e} in config file for SQLAlchemy."
            )
            raise
        except Exception as e:
            print(f"Error creating SQLAlchemy engine: {e}")
            raise
    return _db_engine


def get_db_connection(app_config, new_instance=False, cursor_factory=None):
    global _db_conn
    if not new_instance and _db_conn and not _db_conn.closed:
        # If a specific cursor_factory is requested for an existing global connection,
        # it's tricky. Best to get a new connection if cursor_factory differs or is specified.
        if cursor_factory and type(_db_conn.cursor_factory) != cursor_factory:
            # print("Requested cursor_factory differs from global, creating new connection.")
            pass  # Fall through to new connection logic
        else:
            # print("Reusing existing DB connection.")
            return _db_conn

    # print(f"Establishing new DB connection. new_instance={new_instance}, cursor_factory requested: {cursor_factory is not None}")
    try:
        db_params = {
            "host": app_config["database"]["host"],
            "port": app_config["database"]["port"],
            "dbname": app_config["database"]["dbname"],
            "user": app_config["database"]["user"],
            "password": app_config["database"]["password"],
        }
        if cursor_factory:
            db_params["cursor_factory"] = cursor_factory
        else:
            # Set a default cursor factory if none is provided, e.g., DictCursor
            # If you don't set one, it defaults to tuple-returning cursors.
            db_params["cursor_factory"] = DictCursor

        conn = psycopg2.connect(**db_params)
        conn.autocommit = (
            True  # Or False, depending on your needs. Often True for ingestion.
        )

        if not new_instance:
            _db_conn = (
                conn  # Store as global if not a 'new_instance' request for one-off use
            )
        return conn
    except (psycopg2.Error, KeyError) as e:
        print(f"Database connection error: {e}")
        # Potentially log this error more formally
        return None


def get_binance_client(config_object=None):
    """Initializes a Binance client using the provided config object or by loading default config."""
    current_config = config_object if config_object else load_config()
    try:
        api_key = current_config["binance"].get("api_key", "")
        api_secret = current_config["binance"].get("api_secret", "")
    except KeyError:
        print(
            "Warning: [binance] section not found in config. Using default empty API keys."
        )
        api_key = ""
        api_secret = ""
    return Client(api_key if api_key else None, api_secret if api_secret else None)


def get_or_create_exchange_id(cursor, exchange_name="Binance"):
    """Gets or creates an exchange ID. Requires a psycopg2 cursor."""
    cursor.execute(
        "SELECT exchange_id FROM exchanges WHERE name = %s", (exchange_name,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]  # Direct access if DictCursor gives a dict
    else:
        cursor.execute(
            "INSERT INTO exchanges (name) VALUES (%s) RETURNING exchange_id",
            (exchange_name,),
        )
        return cursor.fetchone()[0]


def get_or_create_symbol_id(
    cursor, exchange_id, instrument_name, base_asset=None, quote_asset=None
):
    """
    Gets or creates a symbol ID. Requires a psycopg2 cursor.
    """
    cursor.execute(
        "SELECT symbol_id, base_asset, quote_asset FROM symbols WHERE exchange_id = %s AND instrument_name = %s",
        (exchange_id, instrument_name),
    )
    result = cursor.fetchone()
    if result:
        return result["symbol_id"]
    else:
        if not base_asset or not quote_asset:
            print(
                f"Warning: Base/Quote not explicitly provided for new symbol {instrument_name}. Attempting inference."
            )
            # (Your inference logic here as before)
            common_quotes = [
                "USDT",
                "USDC",
                "TUSD",
                "BUSD",
                "DAI",
                "BTC",
                "ETH",
                "BNB",
                "EUR",
                "GBP",
                "AUD",
                "TRY",
                "BRL",
            ]
            inferred = False
            for cq in common_quotes:
                if instrument_name.endswith(cq) and len(instrument_name) > len(cq):
                    base = instrument_name[: -len(cq)]
                    if base:
                        base_asset, quote_asset = base, cq
                        inferred = True
                        print(
                            f"Inferred for {instrument_name}: Base={base_asset}, Quote={quote_asset}"
                        )
                        break
            if not inferred:  # Fallback
                base_asset = (
                    instrument_name[:3] if len(instrument_name) > 3 else instrument_name
                )
                quote_asset = (
                    instrument_name[3:] if len(instrument_name) > 3 else "UNKNOWN"
                )
                print(
                    f"Fallback inference for {instrument_name}: Base={base_asset}, Quote={quote_asset}"
                )

        cursor.execute(
            """INSERT INTO symbols (exchange_id, instrument_name, base_asset, quote_asset)
               VALUES (%s, %s, %s, %s) RETURNING symbol_id""",
            (exchange_id, instrument_name, base_asset, quote_asset),
        )
        return cursor.fetchone()["symbol_id"]


def upsert_symbol_extended(
    cursor,
    exchange_id,
    instrument_name,
    base_asset,
    quote_asset,
    api_status=None,
    api_permissions_list=None,
    other_api_details_dict=None,
):
    """Inserts a new symbol or updates details. Requires a psycopg2 cursor."""
    # (Your existing upsert_symbol_extended logic here)
    # This function is not directly impacted by SQLAlchemy for its core logic
    # unless you refactor it to use SQLAlchemy Core/ORM for the upsert.
    # For now, keeping it as is with psycopg2 cursor.
    cursor.execute(
        "SELECT symbol_id FROM symbols WHERE exchange_id = %s AND instrument_name = %s",
        (exchange_id, instrument_name),
    )
    result = cursor.fetchone()
    # ... (rest of your existing logic for upsert) ...
    if result:
        symbol_id = result["symbol_id"]  # Assuming DictCursor
        # Update logic here (as you had)
        return symbol_id, False
    else:
        # Insert logic here (as you had)
        cursor.execute(
            """INSERT INTO symbols (exchange_id, instrument_name, base_asset, quote_asset) 
            VALUES (%s, %s, %s, %s) RETURNING symbol_id""",
            (exchange_id, instrument_name, base_asset, quote_asset),
        )
        symbol_id = cursor.fetchone()["symbol_id"]
        return symbol_id, True
