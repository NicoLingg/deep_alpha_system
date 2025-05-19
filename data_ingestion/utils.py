import os
import psycopg2
import configparser
from binance.client import Client
from psycopg2.extras import DictCursor


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
        # Fallback for when script might be run from a different CWD than project root
        # e.g. if running "python data_ingestion/kline_ingestor.py" directly from project root
        # and --config is not provided, then config_path would be the default relative path.
        # This tries to find it relative to CWD.
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
    """Gets or creates a symbol ID, inferring base/quote assets if not provided."""
    cursor.execute(
        "SELECT symbol_id FROM symbols WHERE exchange_id = %s AND instrument_name = %s",
        (exchange_id, instrument_name),
    )
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        if not base_asset and not quote_asset:
            common_quotes = ["USDT", "USDC", "TUSD", "BUSD", "DAI", "BTC", "ETH", "BNB"]
            inferred = False
            for cq in common_quotes:
                if instrument_name.endswith(cq):
                    base = instrument_name[: -len(cq)]
                    if base:
                        base_asset, quote_asset = base, cq
                        inferred = True
                        break
            if not inferred:
                if len(instrument_name) > 4:  # e.g. BTCUSDT -> BTC, USDT
                    base_asset = (
                        instrument_name[:-4]
                        if instrument_name.endswith("USDT")
                        else instrument_name[:-3]
                    )
                    quote_asset = (
                        instrument_name[-4:]
                        if instrument_name.endswith("USDT")
                        else instrument_name[-3:]
                    )
                elif (
                    len(instrument_name) == 6
                ):  # e.g. BTCETH -> BTC, ETH (common for 3-char pairs)
                    base_asset = instrument_name[:3]
                    quote_asset = instrument_name[3:]
                else:
                    print(
                        f"Warning: Could not reliably infer base/quote for {instrument_name}. Using fallback."
                    )
                    base_asset = instrument_name
                    quote_asset = "UNKNOWN"

        elif not base_asset or not quote_asset:
            print(
                f"Warning: Only one of base_asset or quote_asset provided for {instrument_name}. Both are needed."
            )
            base_asset = base_asset or "UNKNOWN_BASE"
            quote_asset = quote_asset or "UNKNOWN_QUOTE"

        cursor.execute(
            """INSERT INTO symbols (exchange_id, instrument_name, base_asset, quote_asset)
               VALUES (%s, %s, %s, %s) RETURNING symbol_id""",
            (exchange_id, instrument_name, base_asset, quote_asset),
        )
        return cursor.fetchone()[0]
