from flask import Flask, render_template, request, redirect, url_for, flash
from flask_httpauth import HTTPBasicAuth
import psycopg2
from psycopg2.extras import DictCursor
import configparser
import os
from datetime import datetime, timedelta
import json

# --- Configuration ---
PROJECT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_FILE = os.path.join(PROJECT_ROOT_DIR, "config", "config.ini")

if not os.path.exists(CONFIG_FILE):
    alt_config_file_from_cwd = os.path.join(os.getcwd(), "config", "config.ini")
    if os.path.exists(alt_config_file_from_cwd):
        CONFIG_FILE = alt_config_file_from_cwd
        print(f"Warning: Used fallback config path from CWD: {CONFIG_FILE}")
    else:
        raise FileNotFoundError(
            f"Config file not found. Checked primary: {CONFIG_FILE} and CWD fallback: {alt_config_file_from_cwd}"
        )

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

app = Flask(__name__)
app.secret_key = os.urandom(24)
auth = HTTPBasicAuth()

# --- Load WebUI Authentication Credentials ---
auth_enabled = False
WEBUI_USERNAME = None
WEBUI_PASSWORD = None

if (
    "webui_auth" in config
    and "username" in config["webui_auth"]
    and "password" in config["webui_auth"]
):
    WEBUI_USERNAME = config["webui_auth"].get("username")
    WEBUI_PASSWORD = config["webui_auth"].get("password")
    if WEBUI_USERNAME and WEBUI_PASSWORD:
        auth_enabled = True
        app.logger.info("WebUI authentication enabled.")
    else:
        app.logger.warning(
            "WebUI username/password in config.ini are empty. Authentication disabled."
        )
else:
    app.logger.info(
        "WebUI authentication section not found or incomplete in config.ini. Authentication disabled."
    )


# --- Password Verification Callback ---
@auth.verify_password
def verify_password(username, password):
    if auth_enabled:
        if username == WEBUI_USERNAME and password == WEBUI_PASSWORD:
            return username
        return None
    return "anonymous"  # Auth not enabled, allow access without credentials


# --- Database Connection Helper (remains the same) ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=config["database"]["host"],
            port=config["database"]["port"],
            dbname=config["database"]["dbname"],
            user=config["database"]["user"],
            password=config["database"]["password"],
            cursor_factory=DictCursor,
        )
        return conn
    except Exception as e:
        app.logger.error(f"Database connection failed: {e}")
        flash(f"Database connection error: {e}", "danger")
        return None


# --- Common data fetching functions (get_all_symbols_from_db remains the same) ---
def get_all_symbols_from_db():
    conn = get_db_connection()
    if not conn:
        return []
    symbols_list = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol_id, instrument_name, base_asset, quote_asset FROM symbols ORDER BY instrument_name"
            )
            symbols_list = cur.fetchall()
    except psycopg2.Error as e:
        app.logger.error(f"Error fetching symbols: {e}")
    finally:
        if conn:
            conn.close()
    return symbols_list


# --- AGGREGATED_INTERVALS_MAP (remains the same) ---
AGGREGATED_INTERVALS_MAP = {
    "1m": {
        "view_name": "klines",
        "label": "1 Minute (Raw)",
        "time_column": "time",
        "trades_column": "number_of_trades",
    },
    "5m": {
        "view_name": "klines_5min",
        "label": "5 Minutes",
        "time_column": "bucket_time",
        "trades_column": "trades",
    },
    "10m": {
        "view_name": "klines_10min",
        "label": "10 Minutes",
        "time_column": "bucket_time",
        "trades_column": "trades",
    },
    "15m": {
        "view_name": "klines_15min",
        "label": "15 Minutes",
        "time_column": "bucket_time",
        "trades_column": "trades",
    },
    "30m": {
        "view_name": "klines_30min",
        "label": "30 Minutes",
        "time_column": "bucket_time",
        "trades_column": "trades",
    },
    "1h": {
        "view_name": "klines_1hour",
        "label": "1 Hour",
        "time_column": "bucket_time",
        "trades_column": "trades",
    },
    "6h": {
        "view_name": "klines_6hour",
        "label": "6 Hours",
        "time_column": "bucket_time",
        "trades_column": "trades",
    },
    "1d": {
        "view_name": "klines_1day",
        "label": "1 Day",
        "time_column": "bucket_time",
        "trades_column": "trades",
    },
}


# --- New Statistics Fetching Functions ---
def get_kline_stats_for_symbol(symbol_id):
    stats = {
        "raw_1m_count": 0,
        "raw_1m_min_time": None,
        "raw_1m_max_time": None,
        "aggregates": {},
    }
    if not symbol_id:
        return stats

    conn = get_db_connection()
    if not conn:
        return stats

    try:
        with conn.cursor() as cur:
            # Stats for raw 1m klines
            cur.execute(
                """
                SELECT COUNT(*) as count, MIN(time) as min_time, MAX(time) as max_time
                FROM klines
                WHERE symbol_id = %s AND interval = '1m';
                """,
                (symbol_id,),
            )
            raw_stats = cur.fetchone()
            if raw_stats:
                stats["raw_1m_count"] = raw_stats["count"]
                stats["raw_1m_min_time"] = raw_stats["min_time"]
                stats["raw_1m_max_time"] = raw_stats["max_time"]

            # Stats for continuous aggregates
            for key, info in AGGREGATED_INTERVALS_MAP.items():
                if info["view_name"] != "klines":  # Skip raw, already handled
                    try:  # Use try-except as view might not exist or be populated
                        time_col = info["time_column"]
                        cur.execute(
                            f"""
                            SELECT COUNT(*) as count, MIN({time_col}) as min_time, MAX({time_col}) as max_time
                            FROM {info["view_name"]}
                            WHERE symbol_id = %s;
                            """,
                            (symbol_id,),
                        )
                        agg_stat = cur.fetchone()
                        if agg_stat:
                            stats["aggregates"][key] = {
                                "label": info["label"],
                                "count": agg_stat["count"],
                                "min_time": agg_stat["min_time"],
                                "max_time": agg_stat["max_time"],
                            }
                    except psycopg2.Error as e:
                        app.logger.warning(
                            f"Could not get stats for aggregate {info['view_name']} for symbol {symbol_id}: {e}"
                        )
                        stats["aggregates"][key] = {
                            "label": info["label"],
                            "count": "N/A",
                            "min_time": None,
                            "max_time": None,
                        }
    except psycopg2.Error as e:
        app.logger.error(f"Error fetching kline stats for symbol {symbol_id}: {e}")
    finally:
        if conn:
            conn.close()
    return stats


def get_order_book_stats_for_symbol(symbol_id):
    stats = {"total_snapshots": 0, "min_time": None, "max_time": None}
    if not symbol_id:
        return stats

    conn = get_db_connection()
    if not conn:
        return stats

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) as count, MIN(time) as min_time, MAX(time) as max_time
                FROM order_book_snapshots
                WHERE symbol_id = %s;
                """,
                (symbol_id,),
            )
            db_stats = cur.fetchone()
            if db_stats:
                stats["total_snapshots"] = db_stats["count"]
                stats["min_time"] = db_stats["min_time"]
                stats["max_time"] = db_stats["max_time"]
    except psycopg2.Error as e:
        app.logger.error(f"Error fetching order book stats for symbol {symbol_id}: {e}")
    finally:
        if conn:
            conn.close()
    return stats


# --- Routes ---
@app.route("/")
@auth.login_required  # Protect this route
def index():
    return redirect(url_for("view_klines"))


@app.route("/klines", methods=["GET"])
@auth.login_required  # Protect this route
def view_klines():
    symbols = get_all_symbols_from_db()
    selected_symbol_id = request.args.get("symbol_id", type=int)
    kline_stats = None  # Initialize

    default_start_time_val = (datetime.utcnow() - timedelta(days=1)).strftime(
        "%Y-%m-%dT%H:%M"
    )
    default_end_time_val = datetime.utcnow().strftime("%Y-%m-%dT%H:%M")
    default_limit_val = 100
    default_interval_key = "1h"  # Default to 1h for a common view

    if not symbols and not selected_symbol_id:
        flash(
            "No symbols found in the database. Please ingest some data first.",
            "warning",
        )
        return render_template(
            "klines_view.html",
            symbols=[],
            klines_data=None,
            kline_stats=None,
            aggregated_intervals_map=AGGREGATED_INTERVALS_MAP,
            selected_interval_key=default_interval_key,
            query_error=None,
            limit=default_limit_val,
            default_start_time=default_start_time_val,
            default_end_time=default_end_time_val,
            current_view_info=AGGREGATED_INTERVALS_MAP.get(default_interval_key),
        )

    if not selected_symbol_id and symbols:
        selected_symbol_id = symbols[0]["symbol_id"]

    if selected_symbol_id:  # Fetch stats if a symbol is selected
        kline_stats = get_kline_stats_for_symbol(selected_symbol_id)

    selected_interval_key = request.args.get("interval", default_interval_key)
    if selected_interval_key not in AGGREGATED_INTERVALS_MAP:
        flash(
            f"Invalid interval '{selected_interval_key}' selected. Defaulting.",
            "warning",
        )
        selected_interval_key = default_interval_key

    default_end_dt = datetime.utcnow()
    if selected_interval_key in ["1d", "1w", "1M"]:
        default_start_dt = default_end_dt - timedelta(days=90)
    elif selected_interval_key in ["1h", "6h"]:
        default_start_dt = default_end_dt - timedelta(days=7)
    else:
        default_start_dt = default_end_dt - timedelta(days=1)

    start_time_str = request.args.get(
        "start_time", default_start_dt.strftime("%Y-%m-%dT%H:%M")
    )
    end_time_str = request.args.get(
        "end_time", default_end_dt.strftime("%Y-%m-%dT%H:%M")
    )
    limit = request.args.get("limit", default_limit_val, type=int)
    if limit <= 0:
        limit = default_limit_val

    klines_data = []
    query_error = None
    selected_view_info = AGGREGATED_INTERVALS_MAP[selected_interval_key]

    if selected_symbol_id:
        conn = get_db_connection()
        if conn:
            try:
                start_time_dt = datetime.fromisoformat(start_time_str)
                end_time_dt = datetime.fromisoformat(end_time_str)
                table_or_view_name = selected_view_info["view_name"]
                time_column = selected_view_info["time_column"]
                trades_col = selected_view_info["trades_column"]
                params = [selected_symbol_id, start_time_dt, end_time_dt, limit]
                raw_kline_interval_filter_sql = ""
                if table_or_view_name == "klines":
                    raw_kline_interval_filter_sql = "AND k.interval = %s "
                    params.insert(1, "1m")
                if table_or_view_name == "klines":
                    sql_query = f"""
                        SELECT k.time AS time, k.open_price AS open_price, k.high_price AS high_price,
                            k.low_price AS low_price, k.close_price AS close_price, k.volume AS volume,
                            k.{trades_col} AS number_of_trades
                        FROM {table_or_view_name} k
                        WHERE k.symbol_id = %s {raw_kline_interval_filter_sql}
                          AND k.{time_column} >= %s AND k.{time_column} <= %s
                        ORDER BY k.{time_column} DESC LIMIT %s"""
                else:
                    sql_query = f"""
                        SELECT agg.{time_column} AS time, agg.open AS open_price, agg.high AS high_price,
                            agg.low AS low_price, agg.close AS close_price, agg.volume AS volume,
                            agg.{trades_col} AS number_of_trades
                        FROM {table_or_view_name} agg
                        WHERE agg.symbol_id = %s AND agg.{time_column} >= %s AND agg.{time_column} <= %s
                        ORDER BY agg.{time_column} DESC LIMIT %s"""
                with conn.cursor() as cur:
                    app.logger.debug(
                        f"Executing query: {cur.mogrify(sql_query, tuple(params))}"
                    )
                    cur.execute(sql_query, tuple(params))
                    klines_data = cur.fetchall()
                if not klines_data and not query_error:
                    flash(
                        f"No klines data found for {selected_view_info['label']} for symbol ID {selected_symbol_id} in the selected time range. The continuous aggregate might still be populating.",
                        "info",
                    )
            except ValueError:
                query_error = "Invalid datetime format. Please use YYYY-MM-DDTHH:MM."
                flash(query_error, "danger")
            except KeyError:
                query_error = f"Invalid interval selected or not configured: {selected_interval_key}"
                flash(query_error, "danger")
            except psycopg2.Error as db_e:
                app.logger.error(
                    f"Database error fetching klines from {table_or_view_name}: {db_e}"
                )
                query_error = f"Database error: {db_e}"
                flash(query_error, "danger")
            except Exception as e:
                app.logger.error(
                    f"Unexpected error fetching klines from {table_or_view_name}: {e}"
                )
                query_error = f"An unexpected error occurred: {e}"
                flash(query_error, "danger")
            finally:
                if conn:
                    conn.close()
        else:
            query_error = "Could not connect to the database to fetch klines."
            flash(query_error, "danger")

    return render_template(
        "klines_view.html",
        symbols=symbols,
        klines_data=klines_data,
        kline_stats=kline_stats,
        selected_symbol_id=selected_symbol_id,
        aggregated_intervals_map=AGGREGATED_INTERVALS_MAP,
        selected_interval_key=selected_interval_key,
        current_view_info=selected_view_info,
        default_start_time=start_time_str,
        default_end_time=end_time_str,
        query_error=query_error,
        limit=limit,
    )


@app.route("/order-book", methods=["GET"])
@auth.login_required  # Protect this route
def view_order_book():
    symbols = get_all_symbols_from_db()
    selected_symbol_id = request.args.get("symbol_id", type=int)
    display_levels = request.args.get("display_levels", 10, type=int)
    history_count = request.args.get("history", 1, type=int)
    order_book_stats = None

    if display_levels <= 0:
        display_levels = 10
    if history_count <= 0:
        history_count = 1
    if history_count > 20:
        flash("Displaying a maximum of 20 historical snapshots.", "info")
        history_count = 20

    if not symbols and not selected_symbol_id:
        flash("No symbols found in the database.", "warning")
        return render_template(
            "order_book.html",
            symbols=[],
            snapshots_list=None,
            order_book_stats=None,
            display_levels=display_levels,
            history_count=history_count,
            selected_symbol_id=None,
            query_error=None,
        )

    if not selected_symbol_id and symbols:
        selected_symbol_id = symbols[0]["symbol_id"]

    if selected_symbol_id:  # Fetch stats if a symbol is selected
        order_book_stats = get_order_book_stats_for_symbol(selected_symbol_id)

    snapshots_list = []
    query_error = None

    if selected_symbol_id:
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    # ... (rest of the order book fetching logic from previous version - no changes here) ...
                    cur.execute(
                        """
                        SELECT obs.time, obs.last_update_id, obs.bids, obs.asks, s.instrument_name
                        FROM order_book_snapshots obs
                        JOIN symbols s ON obs.symbol_id = s.symbol_id
                        WHERE obs.symbol_id = %s
                        ORDER BY obs.time DESC
                        LIMIT %s
                        """,
                        (selected_symbol_id, history_count),
                    )
                    snapshots_raw = cur.fetchall()
                    if snapshots_raw:
                        for row_raw in snapshots_raw:
                            snapshot_item = dict(row_raw)
                            if isinstance(snapshot_item["bids"], str):
                                snapshot_item["bids"] = json.loads(
                                    snapshot_item["bids"]
                                )
                            if isinstance(snapshot_item["asks"], str):
                                snapshot_item["asks"] = json.loads(
                                    snapshot_item["asks"]
                                )
                            if not isinstance(snapshot_item["bids"], list):
                                snapshot_item["bids"] = []
                            if not isinstance(snapshot_item["asks"], list):
                                snapshot_item["asks"] = []
                            try:
                                snapshot_item["bids"].sort(
                                    key=lambda x: (
                                        float(x[0])
                                        if isinstance(x, (list, tuple)) and len(x) > 0
                                        else -float("inf")
                                    ),
                                    reverse=True,
                                )
                                snapshot_item["asks"].sort(
                                    key=lambda x: (
                                        float(x[0])
                                        if isinstance(x, (list, tuple)) and len(x) > 0
                                        else float("inf")
                                    )
                                )
                            except (TypeError, IndexError) as sort_e:
                                app.logger.error(
                                    f"Error sorting bids/asks for LUID {snapshot_item['last_update_id']}: {sort_e}"
                                )
                            snapshots_list.append(snapshot_item)
                    else:
                        flash(
                            f"No order book snapshots found for symbol ID {selected_symbol_id}.",
                            "info",
                        )
            except TypeError as te:
                app.logger.error(
                    f"TypeError processing order book data (likely JSON issue): {te}"
                )
                query_error = f"Error processing order book data structure: {te}"
                flash(query_error, "danger")
            except psycopg2.Error as db_e:
                app.logger.error(f"Database error fetching order book snapshot: {db_e}")
                query_error = f"Database error: {db_e}"
                flash(query_error, "danger")
            except Exception as e:
                app.logger.error(f"Error fetching order book snapshot: {e}")
                query_error = f"An unexpected error occurred: {e}"
                flash(query_error, "danger")
            finally:
                if conn:
                    conn.close()
        else:
            query_error = "Could not connect to the database to fetch order book."
            flash(query_error, "danger")

    return render_template(
        "order_book.html",
        symbols=symbols,
        snapshots_list=snapshots_list,
        order_book_stats=order_book_stats,  # Pass stats
        selected_symbol_id=selected_symbol_id,
        display_levels=display_levels,
        history_count=history_count,
        query_error=query_error,
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5004)
