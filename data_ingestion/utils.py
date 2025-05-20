import os
import psycopg2
import configparser
from binance.client import Client
from psycopg2.extras import DictCursor
import json


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.ini")


def load_config(config_path=None):
    """
    Loads the configuration from the specified path or the default path.
    The config_path argument should be an absolute path or relative to the project root.
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


def get_db_connection(config_object=None):
    """Establishes a database connection using the provided config object or by loading default config."""
    if config_object is None:
        config_object = load_config()
    try:
        conn = psycopg2.connect(
            host=config_object["database"]["host"],
            port=config_object["database"]["port"],
            dbname=config_object["database"]["dbname"],
            user=config_object["database"]["user"],
            password=config_object["database"]["password"],
            cursor_factory=DictCursor,
        )
        return conn
    except KeyError as e:
        print(f"Error: Missing database configuration key: {e} in config file.")
        raise
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        raise


def get_binance_client(config_object=None):
    """Initializes a Binance client using the provided config object or by loading default config."""
    if config_object is None:
        config_object = load_config()
    try:
        api_key = config_object["binance"].get("api_key", "")
        api_secret = config_object["binance"].get("api_secret", "")
    except KeyError:
        print(
            "Warning: [binance] section not found in config. Using default empty API keys."
        )
        api_key = ""
        api_secret = ""
    return Client(api_key if api_key else None, api_secret if api_secret else None)


def get_or_create_exchange_id(cursor, exchange_name="Binance"):
    """Gets or creates an exchange ID."""
    cursor.execute(
        "SELECT exchange_id FROM exchanges WHERE name = %s", (exchange_name,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]
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
    Gets or creates a symbol ID.
    If base_asset and quote_asset are provided, they are used.
    Otherwise, it tries to infer them (though inference is less reliable for non-standard pairs).
    This version does NOT update existing base_asset/quote_asset if they differ.
    Returns symbol_id.
    """
    cursor.execute(
        "SELECT symbol_id, base_asset, quote_asset FROM symbols WHERE exchange_id = %s AND instrument_name = %s",
        (exchange_id, instrument_name),
    )
    result = cursor.fetchone()
    if result:
        return result["symbol_id"]
    else:
        # If creating, base_asset and quote_asset are required.
        if not base_asset or not quote_asset:
            # Try to infer if not provided (basic inference)
            print(
                f"Warning: Base/Quote not explicitly provided for new symbol {instrument_name}. Attempting inference."
            )
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
                    if base:  # Ensure base is not empty
                        base_asset, quote_asset = base, cq
                        inferred = True
                        print(
                            f"Inferred for {instrument_name}: Base={base_asset}, Quote={quote_asset}"
                        )
                        break
            if not inferred:
                # Fallback if inference fails or assets are very short
                if len(instrument_name) >= 6 and not any(
                    instrument_name.endswith(q) for q in ["USDT", "BUSD"]
                ):  # e.g. BTCETH
                    base_asset = instrument_name[: len(instrument_name) // 2]
                    quote_asset = instrument_name[len(instrument_name) // 2 :]
                elif instrument_name.endswith("USDT") and len(instrument_name) > 4:
                    base_asset = instrument_name[:-4]
                    quote_asset = "USDT"
                elif instrument_name.endswith("BUSD") and len(instrument_name) > 4:
                    base_asset = instrument_name[:-4]
                    quote_asset = "BUSD"
                else:  # Last resort, likely to be inaccurate for some pairs
                    base_asset = (
                        instrument_name[:3]
                        if len(instrument_name) > 3
                        else instrument_name
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
        return cursor.fetchone()[0]


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
    """
    Inserts a new symbol or updates details if it exists.
    Assumes symbols table has (or will have) columns:
    'api_status' (VARCHAR), 'api_permissions' (JSONB), 'other_api_details' (JSONB).
    Returns (symbol_id, was_newly_inserted_boolean).
    """
    cursor.execute(
        "SELECT symbol_id FROM symbols WHERE exchange_id = %s AND instrument_name = %s",
        (exchange_id, instrument_name),
    )
    result = cursor.fetchone()

    permissions_json = (
        json.dumps(api_permissions_list) if api_permissions_list else None
    )
    other_details_json = (
        json.dumps(other_api_details_dict) if other_api_details_dict else None
    )

    if result:
        symbol_id = result[0]
        update_clauses = []
        params = []

        if base_asset is not None:
            update_clauses.append("base_asset = %s")
            params.append(base_asset)
        if quote_asset is not None:
            update_clauses.append("quote_asset = %s")
            params.append(quote_asset)

        if update_clauses:
            query = (
                f"UPDATE symbols SET {', '.join(update_clauses)} WHERE symbol_id = %s"
            )
            params.append(symbol_id)
            cursor.execute(query, tuple(params))

        return symbol_id, False  # False = updated
    else:
        # --- Adapt INSERT to include new columns if added to schema.sql ---
        query = """
            INSERT INTO symbols (exchange_id, instrument_name, base_asset, quote_asset) 
            VALUES (%s, %s, %s, %s) 
            RETURNING symbol_id
        """
        params_insert = [exchange_id, instrument_name, base_asset, quote_asset]

        cursor.execute(query, tuple(params_insert))
        symbol_id = cursor.fetchone()[0]
        return symbol_id, True  # True = newly inserted
