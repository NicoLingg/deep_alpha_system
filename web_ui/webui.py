# File: web_ui/webui.py
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_httpauth import HTTPBasicAuth
import psycopg2
from psycopg2.extras import DictCursor
import configparser
import os
from datetime import datetime, timedelta, timezone
import json

# --- Configuration --- ( 그대로 )
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

# --- AGGREGATED_INTERVALS_MAP --- ( 그대로 )
AGGREGATED_INTERVALS_MAP = {
    "1m": {
        "view_name": "klines",
        "label": "1 Minute (Raw)",
        "time_column": "time",
        "trades_column": "number_of_trades",
        "is_aggregate": False,
        "default_lookback": timedelta(days=1),
    },
    "5m": {
        "view_name": "klines_5min",
        "label": "5 Minutes",
        "time_column": "bucket_time",
        "trades_column": "trades",
        "is_aggregate": True,
        "default_lookback": timedelta(days=3),
    },
    "10m": {
        "view_name": "klines_10min",
        "label": "10 Minutes",
        "time_column": "bucket_time",
        "trades_column": "trades",
        "is_aggregate": True,
        "default_lookback": timedelta(days=7),
    },
    "15m": {
        "view_name": "klines_15min",
        "label": "15 Minutes",
        "time_column": "bucket_time",
        "trades_column": "trades",
        "is_aggregate": True,
        "default_lookback": timedelta(days=7),
    },
    "30m": {
        "view_name": "klines_30min",
        "label": "30 Minutes",
        "time_column": "bucket_time",
        "trades_column": "trades",
        "is_aggregate": True,
        "default_lookback": timedelta(days=14),
    },
    "1h": {
        "view_name": "klines_1hour",
        "label": "1 Hour",
        "time_column": "bucket_time",
        "trades_column": "trades",
        "is_aggregate": True,
        "default_lookback": timedelta(days=30),
    },
    "6h": {
        "view_name": "klines_6hour",
        "label": "6 Hours",
        "time_column": "bucket_time",
        "trades_column": "trades",
        "is_aggregate": True,
        "default_lookback": timedelta(days=90),
    },
    "1d": {
        "view_name": "klines_1day",
        "label": "1 Day",
        "time_column": "bucket_time",
        "trades_column": "trades",
        "is_aggregate": True,
        "default_lookback": timedelta(days=365),
    },
}


# --- Context Processor --- ( 그대로 )
@app.context_processor
def inject_global_vars():
    return dict(
        now=datetime.utcnow,
        AGGREGATED_INTERVALS_MAP=AGGREGATED_INTERVALS_MAP,
    )


# --- Auth --- ( 그대로 )
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


@auth.verify_password
def verify_password(username, password):
    if auth_enabled:
        if username == WEBUI_USERNAME and password == WEBUI_PASSWORD:
            return username
        return None
    return "anonymous"


# --- DB Connection Helper --- ( 그대로 )
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


# --- Common data fetching functions --- ( 그대로 )
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
        flash(f"Error fetching symbols from DB: {e}", "danger")
    finally:
        if conn:
            conn.close()
    return symbols_list


# --- Overview Page Logic (Existing Stats) --- ( 그대로 )
def get_dataset_overview_stats():
    stats = {
        "total_symbols": 0,
        "kline_stats_by_interval": {},
        "order_book_stats": {"count": 0, "min_time": None, "max_time": None},
    }
    conn = get_db_connection()
    if not conn:
        return stats
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM symbols;")
            symbol_count_res = cur.fetchone()
            if symbol_count_res:
                stats["total_symbols"] = symbol_count_res["count"]
            for key, info in AGGREGATED_INTERVALS_MAP.items():
                table_or_view = info["view_name"]
                time_col = info["time_column"]
                query = f"SELECT COUNT(*) as count, MIN({time_col}) as min_time, MAX({time_col}) as max_time FROM {table_or_view}"
                params = (key,) if not info["is_aggregate"] else ()
                if not info["is_aggregate"]:
                    query += " WHERE interval = %s"
                try:
                    cur.execute(query, params)
                    res = cur.fetchone()
                    stats["kline_stats_by_interval"][key] = {
                        "label": info["label"],
                        "count": res["count"] if res else 0,
                        "min_time": (
                            res["min_time"] if res and res["min_time"] else None
                        ),
                        "max_time": (
                            res["max_time"] if res and res["max_time"] else None
                        ),
                    }
                except psycopg2.Error as e:
                    app.logger.warning(
                        f"Could not query {table_or_view} for overview: {e}"
                    )
                    stats["kline_stats_by_interval"][key] = {
                        "label": info["label"],
                        "count": "Error",
                        "min_time": None,
                        "max_time": None,
                    }
            cur.execute(
                "SELECT COUNT(*) as count, MIN(time) as min_time, MAX(time) as max_time FROM order_book_snapshots;"
            )
            ob_res = cur.fetchone()
            if ob_res:
                stats["order_book_stats"]["count"] = ob_res["count"]
                stats["order_book_stats"]["min_time"] = ob_res["min_time"]
                stats["order_book_stats"]["max_time"] = ob_res["max_time"]
    except psycopg2.Error as e:
        app.logger.error(f"Error fetching dataset overview stats: {e}")
        flash(f"Error fetching overview stats: {e}", "danger")
    finally:
        if conn:
            conn.close()
    return stats


# --- NEW Plot Data Fetching Functions ---
def get_plot_data_quote_asset_distribution(conn):
    labels, data = [], []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT quote_asset, COUNT(*) AS symbol_count FROM symbols GROUP BY quote_asset ORDER BY symbol_count DESC;"
            )
            for row in cur.fetchall():
                labels.append(row["quote_asset"])
                data.append(row["symbol_count"])
    except psycopg2.Error as e:
        app.logger.error(f"Error fetching quote asset distribution for plot: {e}")
    return {"labels": labels, "data": data}


def get_plot_data_top_symbols_by_klines(conn, limit=10):
    labels, data = [], []
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT s.instrument_name, COUNT(k.symbol_id) AS kline_count 
                FROM klines k JOIN symbols s ON k.symbol_id = s.symbol_id 
                WHERE k.interval = '1m' GROUP BY s.instrument_name 
                ORDER BY kline_count DESC LIMIT %s;
            """,
                (limit,),
            )
            for row in cur.fetchall():
                labels.append(row["instrument_name"])
                data.append(row["kline_count"])
    except psycopg2.Error as e:
        app.logger.error(f"Error fetching top symbols by klines for plot: {e}")
    return {"labels": labels, "data": data}


def get_plot_data_daily_ingestion_counts(
    conn, table_name, time_column, days=30, kline_interval_filter=None
):
    labels, data_values = (
        [],
        [],
    )  # Use data_values to avoid conflict with 'data' var name in JS
    try:
        with conn.cursor() as cur:
            base_query = f"SELECT DATE_TRUNC('day', {time_column}) AS day_bucket, COUNT(*) AS daily_count FROM {table_name}"
            conditions = [f"{time_column} >= NOW() - INTERVAL '{days} days'"]
            params = []
            if kline_interval_filter and table_name == "klines":
                conditions.append("interval = %s")
                params.append(kline_interval_filter)

            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)

            base_query += " GROUP BY day_bucket ORDER BY day_bucket ASC;"
            cur.execute(base_query, tuple(params))

            results_map = {
                row["day_bucket"].strftime("%Y-%m-%d"): row["daily_count"]
                for row in cur.fetchall()
            }

            start_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            ) - timedelta(days=days - 1)
            for i in range(days):
                current_day_dt = start_date + timedelta(days=i)
                day_str = current_day_dt.strftime("%Y-%m-%d")
                labels.append(day_str)
                data_values.append(results_map.get(day_str, 0))

    except psycopg2.Error as e:
        app.logger.error(
            f"Error fetching daily {table_name} (col: {time_column}) counts for plot: {e}"
        )
    return {"labels": labels, "data": data_values}


# --- Kline Page Logic --- ( 그대로 )
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
            cur.execute(
                "SELECT COUNT(*) as count, MIN(time) as min_time, MAX(time) as max_time FROM klines WHERE symbol_id = %s AND interval = '1m';",
                (symbol_id,),
            )
            raw_stats_res = cur.fetchone()
            if raw_stats_res:
                stats["raw_1m_count"] = raw_stats_res["count"]
                stats["raw_1m_min_time"] = raw_stats_res["min_time"]
                stats["raw_1m_max_time"] = raw_stats_res["max_time"]
            for key, info in AGGREGATED_INTERVALS_MAP.items():
                if info["is_aggregate"]:
                    try:
                        cur.execute(
                            f"SELECT COUNT(*) as count, MIN({info['time_column']}) as min_time, MAX({info['time_column']}) as max_time FROM {info['view_name']} WHERE symbol_id = %s;",
                            (symbol_id,),
                        )
                        agg_res = cur.fetchone()
                        stats["aggregates"][key] = {
                            "label": info["label"],
                            "count": agg_res["count"] if agg_res else 0,
                            "min_time": agg_res["min_time"] if agg_res else None,
                            "max_time": agg_res["max_time"] if agg_res else None,
                        }
                    except psycopg2.Error:
                        stats["aggregates"][key] = {
                            "label": info["label"],
                            "count": 0,
                            "min_time": None,
                            "max_time": None,
                        }
    except psycopg2.Error as e:
        app.logger.error(f"Error fetching kline stats for symbol {symbol_id}: {e}")
    finally:
        if conn:
            conn.close()
    return stats


def get_latest_kline_timestamp(conn, symbol_id, interval_key):
    if not symbol_id or not interval_key or not conn:
        return None
    interval_info = AGGREGATED_INTERVALS_MAP.get(interval_key)
    if not interval_info:
        return None
    table_or_view = interval_info["view_name"]
    time_col = interval_info["time_column"]
    query = (
        f"SELECT MAX({time_col}) as max_time FROM {table_or_view} WHERE symbol_id = %s"
    )
    params = [symbol_id]
    if not interval_info["is_aggregate"]:
        query += " AND interval = %s"
        params.append(interval_key)
    try:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            result = cur.fetchone()
            return result["max_time"] if result and result["max_time"] else None
    except psycopg2.Error as e:
        app.logger.error(
            f"Error fetching latest kline timestamp for {symbol_id}, {interval_key}: {e}"
        )
        return None


# --- Order Book Page Logic --- ( 그대로 )
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
                "SELECT COUNT(*) as count, MIN(time) as min_time, MAX(time) as max_time FROM order_book_snapshots WHERE symbol_id = %s;",
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
@auth.login_required
def index():
    return redirect(url_for("dataset_overview"))


@app.route("/overview")
@auth.login_required
def dataset_overview():
    overview_stats_table_data = get_dataset_overview_stats()  # Data for existing tables

    # Data for new plots
    plot_data_for_template = {
        "quote_asset_dist_json": "{}",  # Default to empty JSON string
        "top_symbols_klines_json": "{}",
        "daily_klines_ingested_json": "{}",
        "daily_snapshots_ingested_json": "{}",
    }
    conn = get_db_connection()
    if conn:
        try:
            plot_data_for_template["quote_asset_dist_json"] = json.dumps(
                get_plot_data_quote_asset_distribution(conn)
            )
            plot_data_for_template["top_symbols_klines_json"] = json.dumps(
                get_plot_data_top_symbols_by_klines(conn, limit=10)
            )
            plot_data_for_template["daily_klines_ingested_json"] = json.dumps(
                get_plot_data_daily_ingestion_counts(
                    conn, "klines", "ingested_at", days=30, kline_interval_filter="1m"
                )
            )
            plot_data_for_template["daily_snapshots_ingested_json"] = json.dumps(
                get_plot_data_daily_ingestion_counts(
                    conn, "order_book_snapshots", "retrieved_at", days=30
                )
            )
        except Exception as e:
            app.logger.error(f"Error preparing plot data for overview: {e}")
            flash("Could not load some chart data due to an internal error.", "warning")
        finally:
            conn.close()
    else:
        flash("Database connection failed, charts cannot be loaded.", "danger")

    return render_template(
        "overview.html",
        overview_stats=overview_stats_table_data,
        plot_data=plot_data_for_template,  # Pass the dict containing JSON strings for plots
    )


# --- /klines route --- ( 그대로 )
@app.route("/klines", methods=["GET"])
@auth.login_required
def view_klines():
    symbols = get_all_symbols_from_db()
    selected_symbol_id = request.args.get("symbol_id", type=int)
    selected_interval_key = request.args.get("interval", "1h")
    limit = request.args.get("limit", 100, type=int)
    if limit <= 0 or limit > 2000:
        limit = 100
    user_start_time_str = request.args.get("start_time")
    user_end_time_str = request.args.get("end_time")
    kline_stats = None
    klines_data = []
    chart_data_json = "{}"
    query_error = None
    start_time_for_input = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
        "%Y-%m-%dT%H:%M"
    )
    end_time_for_input = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    if selected_interval_key not in AGGREGATED_INTERVALS_MAP:
        flash(
            f"Invalid interval '{selected_interval_key}'. Defaulting to 1h.", "warning"
        )
        selected_interval_key = "1h"
    current_view_info = AGGREGATED_INTERVALS_MAP[selected_interval_key]
    if not symbols:
        flash(
            "No symbols found in the database. Please ingest symbol definitions first.",
            "warning",
        )
    elif not selected_symbol_id and symbols:
        selected_symbol_id = symbols[0]["symbol_id"]
    if selected_symbol_id:
        kline_stats = get_kline_stats_for_symbol(selected_symbol_id)
        conn = get_db_connection()
        if conn:
            latest_data_ts = get_latest_kline_timestamp(
                conn, selected_symbol_id, selected_interval_key
            )
            default_lookback = current_view_info.get(
                "default_lookback", timedelta(days=30)
            )
            query_start_time_dt, query_end_time_dt = None, None
            parsed_user_start, parsed_user_end = None, None
            try:
                if user_start_time_str:
                    parsed_user_start = datetime.fromisoformat(
                        user_start_time_str
                    ).replace(tzinfo=timezone.utc)
                if user_end_time_str:
                    parsed_user_end = datetime.fromisoformat(user_end_time_str).replace(
                        tzinfo=timezone.utc
                    )
                if parsed_user_start and parsed_user_end:
                    if parsed_user_start >= parsed_user_end:
                        flash("Start time must be before end time.", "warning")
                        parsed_user_start = parsed_user_end = None
                    else:
                        query_start_time_dt, query_end_time_dt = (
                            parsed_user_start,
                            parsed_user_end,
                        )
                elif parsed_user_start:
                    query_start_time_dt = parsed_user_start
                    query_end_time_dt = (
                        latest_data_ts if latest_data_ts else datetime.now(timezone.utc)
                    )
                elif parsed_user_end:
                    query_end_time_dt = parsed_user_end
                    query_start_time_dt = query_end_time_dt - default_lookback
            except ValueError:
                flash("Invalid date format. Use YYYY-MM-DDTHH:MM.", "warning")
                parsed_user_start = parsed_user_end = None
            if not query_start_time_dt or not query_end_time_dt:
                if latest_data_ts:
                    query_end_time_dt = latest_data_ts
                    query_start_time_dt = query_end_time_dt - default_lookback
                else:
                    query_end_time_dt = datetime.now(timezone.utc)
                    query_start_time_dt = query_end_time_dt - default_lookback
            start_time_for_input = query_start_time_dt.strftime("%Y-%m-%dT%H:%M")
            end_time_for_input = query_end_time_dt.strftime("%Y-%m-%dT%H:%M")
            try:
                table_or_view_name = current_view_info["view_name"]
                time_column = current_view_info["time_column"]
                trades_col = current_view_info["trades_column"]
                open_col, high_col, low_col, close_col = (
                    ("open_price", "high_price", "low_price", "close_price")
                    if not current_view_info["is_aggregate"]
                    else ("open", "high", "low", "close")
                )
                sql_params = [
                    selected_symbol_id,
                    query_start_time_dt,
                    query_end_time_dt,
                    limit,
                ]
                interval_filter_sql = ""
                if not current_view_info["is_aggregate"]:
                    interval_filter_sql = "AND interval = %s "
                    sql_params.insert(1, selected_interval_key)
                sql_query = f"SELECT {time_column} AS time, {open_col} AS open_price, {high_col} AS high_price, {low_col} AS low_price, {close_col} AS close_price, volume, {trades_col} AS number_of_trades FROM {table_or_view_name} WHERE symbol_id = %s {interval_filter_sql} AND {time_column} >= %s AND {time_column} <= %s ORDER BY {time_column} DESC LIMIT %s"
                with conn.cursor() as cur:
                    cur.execute(sql_query, tuple(sql_params))
                    klines_data = cur.fetchall()
                if klines_data:
                    chart_labels, chart_close_prices, chart_volumes = [], [], []
                    for kr in reversed(klines_data):
                        chart_labels.append(kr["time"].isoformat())
                        chart_close_prices.append(float(kr["close_price"]))
                        chart_volumes.append(float(kr["volume"]))
                    chart_data_json = json.dumps(
                        {
                            "labels": chart_labels,
                            "close_prices": chart_close_prices,
                            "volumes": chart_volumes,
                        }
                    )
                elif not query_error:
                    flash("No klines data found for the selected criteria.", "info")
            except psycopg2.Error as db_e:
                app.logger.error(f"DB error fetching klines: {db_e}")
                query_error = f"Database error: {str(db_e)[:200]}..."
                flash(query_error, "danger")
            except Exception as e:
                app.logger.error(f"Unexpected error fetching klines: {e}")
                query_error = f"An unexpected error: {e}"
                flash(query_error, "danger")
            finally:
                if conn:
                    conn.close()
        else:
            query_error = "Database connection failed."
    return render_template(
        "klines_view.html",
        symbols=symbols,
        klines_data=klines_data,
        kline_stats=kline_stats,
        selected_symbol_id=selected_symbol_id,
        selected_interval_key=selected_interval_key,
        current_view_info=current_view_info,
        start_time_for_input=start_time_for_input,
        end_time_for_input=end_time_for_input,
        limit=limit,
        chart_data_json=chart_data_json,
        query_error=query_error,
    )


# --- /order-book route --- ( 그대로 )
@app.route("/order-book", methods=["GET"])
@auth.login_required
def view_order_book():
    symbols = get_all_symbols_from_db()
    selected_symbol_id = request.args.get("symbol_id", type=int)
    display_levels = request.args.get("display_levels", 10, type=int)
    history_count = request.args.get("history", 1, type=int)
    order_book_stats = None
    snapshots_list = []
    query_error = None
    if display_levels <= 0 or display_levels > 100:
        display_levels = 10
    if history_count <= 0 or history_count > 20:
        history_count = 1
    if not symbols:
        flash("No symbols found in the database.", "warning")
    elif not selected_symbol_id and symbols:
        selected_symbol_id = symbols[0]["symbol_id"]
    if selected_symbol_id:
        order_book_stats = get_order_book_stats_for_symbol(selected_symbol_id)
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT obs.time, obs.last_update_id, obs.bids, obs.asks, s.instrument_name FROM order_book_snapshots obs JOIN symbols s ON obs.symbol_id = s.symbol_id WHERE obs.symbol_id = %s ORDER BY obs.time DESC LIMIT %s",
                        (selected_symbol_id, history_count),
                    )
                    snapshots_raw = cur.fetchall()
                    if snapshots_raw:
                        for row_raw in snapshots_raw:
                            snapshot_item = dict(row_raw)
                            snapshot_item["bids"] = (
                                json.loads(snapshot_item["bids"])
                                if isinstance(snapshot_item["bids"], str)
                                else (snapshot_item["bids"] or [])
                            )
                            snapshot_item["asks"] = (
                                json.loads(snapshot_item["asks"])
                                if isinstance(snapshot_item["asks"], str)
                                else (snapshot_item["asks"] or [])
                            )
                            try:
                                snapshot_item["bids"].sort(
                                    key=lambda x: float(x[0]), reverse=True
                                )
                                snapshot_item["asks"].sort(key=lambda x: float(x[0]))
                            except (IndexError, TypeError, ValueError) as e:
                                app.logger.error(
                                    f"Could not sort bids/asks for snapshot {snapshot_item['last_update_id']}: {e}"
                                )
                            snapshots_list.append(snapshot_item)
                    elif (
                        not order_book_stats
                        or order_book_stats.get("total_snapshots", 0) == 0
                    ):
                        flash("No order book snapshots found for this symbol.", "info")
            except json.JSONDecodeError as je:
                app.logger.error(f"JSONDecodeError for order book data: {je}")
                query_error = "Error decoding order book data."
                flash(query_error, "danger")
            except psycopg2.Error as db_e:
                app.logger.error(f"DB error fetching order book: {db_e}")
                query_error = f"Database error: {str(db_e)[:200]}..."
                flash(query_error, "danger")
            except Exception as e:
                app.logger.error(f"Unexpected error fetching order book: {e}")
                query_error = f"An unexpected error: {e}"
                flash(query_error, "danger")
            finally:
                if conn:
                    conn.close()
        else:
            query_error = "Database connection failed."
    return render_template(
        "order_book.html",
        symbols=symbols,
        snapshots_list=snapshots_list,
        order_book_stats=order_book_stats,
        selected_symbol_id=selected_symbol_id,
        display_levels=display_levels,
        history_count=history_count,
        query_error=query_error,
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
