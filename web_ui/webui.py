# File: web_ui/webui.py
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_httpauth import HTTPBasicAuth
import psycopg2
from psycopg2.extras import DictCursor
import configparser
import os
from datetime import datetime, timedelta, timezone
import json

# --- Configuration ---
PROJECT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_FILE = os.path.join(PROJECT_ROOT_DIR, "config", "config.ini")

app = Flask(__name__)

if not os.path.exists(CONFIG_FILE):
    alt_config_file_from_cwd = os.path.join(os.getcwd(), "config", "config.ini")
    if os.path.exists(alt_config_file_from_cwd):
        CONFIG_FILE = alt_config_file_from_cwd
        app.logger.warning(
            f"Warning: Used fallback config path from CWD: {CONFIG_FILE}"
        )
    else:
        # This will be an issue if app.logger isn't available yet, print instead
        print(
            f"CRITICAL ERROR: Config file not found. Checked primary: {os.path.join(PROJECT_ROOT_DIR, 'config', 'config.ini')} and CWD fallback: {alt_config_file_from_cwd}"
        )
        raise FileNotFoundError("Config file not found.")


config = configparser.ConfigParser()
config.read(CONFIG_FILE)

app.secret_key = os.urandom(
    24
)  # TODO: using a fixed secret for dev or environment variable for prod
auth = HTTPBasicAuth()

# --- AGGREGATED_INTERVALS_MAP ---
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


# --- Context Processor ---
@app.context_processor
def inject_global_vars():
    return dict(
        now=datetime.utcnow,
        AGGREGATED_INTERVALS_MAP=AGGREGATED_INTERVALS_MAP,
    )


# --- Auth ---
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
    return "anonymous"  # Allow access if auth is disabled


# --- DB Connection Helper ---
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


# --- Common data fetching functions ---
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


# --- Overview Page Logic ---
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
                stats["total_symbols"] = (
                    symbol_count_res["count"]
                    if symbol_count_res["count"] is not None
                    else 0
                )

            for key, info in AGGREGATED_INTERVALS_MAP.items():
                table_or_view = info["view_name"]
                time_col = info["time_column"]

                current_stat = {
                    "label": info["label"],
                    "count": 0,
                    "min_time": None,
                    "max_time": None,
                }
                try:
                    # Check if the table/view exists
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM pg_class WHERE relname = %s AND relkind IN ('r', 'p', 'v', 'm', 'f')
                        );
                    """,
                        (table_or_view,),
                    )
                    source_exists = cur.fetchone()[0]

                    if not source_exists:
                        app.logger.warning(
                            f"Source '{table_or_view}' for interval {key} not found. Stats will show 'Source Missing'."
                        )
                        current_stat["count"] = "Source Missing"
                        stats["kline_stats_by_interval"][key] = current_stat
                        continue  # Skip to next interval if source doesn't exist

                    if key == "1m":
                        cur.execute(
                            """
                            SELECT SUM(kline_1m_count) as total_count, 
                                   MIN(first_1m_kline_time) as overall_min_time, 
                                   MAX(last_1m_kline_time) as overall_max_time 
                            FROM symbol_1m_kline_counts
                            WHERE kline_1m_count > 0; 
                        """
                        )
                        res = cur.fetchone()
                        if res:
                            current_stat["count"] = (
                                res["total_count"]
                                if res["total_count"] is not None
                                else 0
                            )
                            current_stat["min_time"] = res["overall_min_time"]
                            current_stat["max_time"] = res["overall_max_time"]
                        else:
                            current_stat["count"] = 0
                    else:
                        query = f"SELECT COUNT(*) as count, MIN({time_col}) as min_time, MAX({time_col}) as max_time FROM {table_or_view}"
                        cur.execute(query)
                        res = cur.fetchone()
                        if res:
                            current_stat["count"] = (
                                res["count"] if res["count"] is not None else 0
                            )
                            current_stat["min_time"] = res["min_time"]
                            current_stat["max_time"] = res["max_time"]
                        else:
                            current_stat["count"] = 0

                    stats["kline_stats_by_interval"][key] = current_stat

                except psycopg2.Error as e_interval:
                    app.logger.warning(
                        f"DB Error querying stats for interval {key} ({table_or_view}): {e_interval}"
                    )
                    current_stat["count"] = "DB Error"
                    stats["kline_stats_by_interval"][key] = current_stat

            cur.execute(
                "SELECT COUNT(*) as count, MIN(time) as min_time, MAX(time) as max_time FROM order_book_snapshots;"
            )
            ob_res = cur.fetchone()
            if ob_res:
                stats["order_book_stats"]["count"] = (
                    ob_res["count"] if ob_res["count"] is not None else 0
                )
                stats["order_book_stats"]["min_time"] = ob_res["min_time"]
                stats["order_book_stats"]["max_time"] = ob_res["max_time"]

    except psycopg2.Error as e:
        app.logger.error(f"Error fetching dataset overview stats: {e}")
        flash(f"Error fetching overview stats: {e}", "danger")
    finally:
        if conn:
            conn.close()
    return stats


# --- Plot Data Fetching Functions ---
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
    mv_name = "symbol_1m_kline_counts"
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = %s AND relkind IN ('m')
                );
            """,
                (mv_name,),
            )  # Specifically check for materialized view 'm'
            if not cur.fetchone()[0]:
                app.logger.warning(
                    f"Materialized view '{mv_name}' not found for top symbols plot."
                )
                return {"labels": labels, "data": data}

            cur.execute(
                f"""
                SELECT instrument_name, kline_1m_count 
                FROM {mv_name}
                WHERE kline_1m_count > 0 AND kline_1m_count IS NOT NULL
                ORDER BY kline_1m_count DESC
                LIMIT %s;
            """,
                (limit,),
            )
            for row in cur.fetchall():
                labels.append(row["instrument_name"])
                data.append(row["kline_1m_count"])
    except psycopg2.Error as e:
        app.logger.error(
            f"DB Error fetching top symbols by klines (from {mv_name}): {e}"
        )
    return {"labels": labels, "data": data}


def get_plot_data_daily_ingestion_counts(conn, data_type, days=30):
    labels, data_values = [], []

    view_name_map = {
        "klines_1m": "daily_1m_klines_ingested_stats",
        "snapshots": "daily_order_book_snapshots_ingested_stats",
    }
    view_name = view_name_map.get(data_type)

    if not view_name:
        app.logger.error(f"Unknown data_type for daily ingestion counts: {data_type}")
        return {"labels": labels, "data": data_values}

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = %s AND relkind IN ('m')
                );
            """,
                (view_name,),
            )  # Check if the specific MV exists
            if not cur.fetchone()[0]:
                app.logger.warning(
                    f"Materialized view '{view_name}' not found for daily {data_type} plot."
                )
                return {"labels": labels, "data": data_values}

            end_date_exclusive = (datetime.now(timezone.utc) + timedelta(days=1)).date()
            start_date_inclusive = end_date_exclusive - timedelta(days=days)

            query = f"""
                SELECT day_bucket, daily_count
                FROM {view_name}
                WHERE day_bucket >= %s AND day_bucket < %s
                ORDER BY day_bucket ASC;
            """
            cur.execute(query, (start_date_inclusive, end_date_exclusive))

            results_map = {
                row["day_bucket"].strftime("%Y-%m-%d"): row["daily_count"]
                for row in cur.fetchall()
            }

            for i in range(days):
                current_day_dt = start_date_inclusive + timedelta(days=i)
                day_str = current_day_dt.strftime("%Y-%m-%d")
                labels.append(day_str)
                data_values.append(results_map.get(day_str, 0))

    except psycopg2.Error as e:
        app.logger.error(
            f"DB Error fetching daily {data_type} counts (from {view_name}): {e}"
        )
    return {"labels": labels, "data": data_values}


# --- Kline Page Logic ---
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
                stats["raw_1m_count"] = (
                    raw_stats_res["count"] if raw_stats_res["count"] is not None else 0
                )
                stats["raw_1m_min_time"] = raw_stats_res["min_time"]
                stats["raw_1m_max_time"] = raw_stats_res["max_time"]

            for key, info in AGGREGATED_INTERVALS_MAP.items():
                if info["is_aggregate"]:
                    agg_stat = {
                        "label": info["label"],
                        "count": 0,
                        "min_time": None,
                        "max_time": None,
                    }
                    table_or_view = info["view_name"]
                    time_col = info["time_column"]
                    try:
                        cur.execute(
                            """
                            SELECT EXISTS (
                                SELECT 1 FROM pg_class WHERE relname = %s AND relkind IN ('m', 'v')
                            );
                        """,
                            (table_or_view,),
                        )  # Check for 'm' (materialized view) or 'v' (regular view)
                        if cur.fetchone()[0]:
                            cur.execute(
                                f"SELECT COUNT(*) as count, MIN({time_col}) as min_time, MAX({time_col}) as max_time FROM {table_or_view} WHERE symbol_id = %s;",
                                (symbol_id,),
                            )
                            agg_res = cur.fetchone()
                            if agg_res:
                                agg_stat["count"] = (
                                    agg_res["count"]
                                    if agg_res["count"] is not None
                                    else 0
                                )
                                agg_stat["min_time"] = agg_res["min_time"]
                                agg_stat["max_time"] = agg_res["max_time"]
                        else:
                            agg_stat["count"] = "Source Missing"
                    except psycopg2.Error as e_agg:
                        app.logger.warning(
                            f"Error querying CAGG {table_or_view} for symbol {symbol_id}: {e_agg}"
                        )
                        agg_stat["count"] = "DB Error"
                    stats["aggregates"][key] = agg_stat
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

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = %s AND relkind IN ('r', 'p', 'v', 'm', 'f')
                );
            """,
                (table_or_view,),
            )  # 'r' table, 'p' partitioned, 'v' view, 'm' mat view, 'f' foreign
            if not cur.fetchone()[0]:
                app.logger.warning(
                    f"Table/View '{table_or_view}' for latest timestamp check does not exist."
                )
                return None

            query = f"SELECT MAX({time_col}) as max_time FROM {table_or_view} WHERE symbol_id = %s"
            params = [symbol_id]
            if not interval_info["is_aggregate"]:
                query += " AND interval = %s"
                params.append(interval_key)

            cur.execute(query, tuple(params))
            result = cur.fetchone()
            return result["max_time"] if result and result["max_time"] else None
    except psycopg2.Error as e:
        app.logger.error(
            f"Error fetching latest kline timestamp for {symbol_id} ({interval_key}) from {table_or_view}: {e}"
        )
        return None


# --- Order Book Page Logic ---
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
                stats["total_snapshots"] = (
                    db_stats["count"] if db_stats["count"] is not None else 0
                )
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
    overview_stats_table_data = get_dataset_overview_stats()
    plot_data_for_template = {
        "quote_asset_dist_json": "{}",
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
                get_plot_data_daily_ingestion_counts(conn, "klines_1m", days=30)
            )
            plot_data_for_template["daily_snapshots_ingested_json"] = json.dumps(
                get_plot_data_daily_ingestion_counts(conn, "snapshots", days=30)
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
        plot_data=plot_data_for_template,
    )


@app.route("/klines", methods=["GET"])
@auth.login_required
def view_klines():
    symbols = get_all_symbols_from_db()
    selected_symbol_id = request.args.get("symbol_id", type=int)
    selected_interval_key = request.args.get("interval", "1h")
    limit = request.args.get("limit", 100, type=int)
    if limit <= 0 or limit > 2000:
        limit = 100

    kline_stats = None
    klines_data = []
    chart_data_json = "{}"
    query_error = None

    default_end_dt = datetime.now(timezone.utc)
    # Determine default lookback based on selected interval or a general default
    current_interval_info_temp = AGGREGATED_INTERVALS_MAP.get(
        selected_interval_key, AGGREGATED_INTERVALS_MAP["1h"]
    )  # Fallback to 1h for default lookback
    default_lookback_td = current_interval_info_temp.get(
        "default_lookback", timedelta(days=7)
    )
    default_start_dt = default_end_dt - default_lookback_td

    start_time_for_input = request.args.get(
        "start_time", default_start_dt.strftime("%Y-%m-%dT%H:%M")
    )
    end_time_for_input = request.args.get(
        "end_time", default_end_dt.strftime("%Y-%m-%dT%H:%M")
    )

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
        # If symbol changes via dropdown, reset date inputs to defaults for that interval
        if request.args.get("symbol_id"):  # Indicates symbol was explicitly changed
            conn_temp = get_db_connection()
            latest_data_ts = None
            if conn_temp:
                latest_data_ts = get_latest_kline_timestamp(
                    conn_temp, selected_symbol_id, selected_interval_key
                )
                conn_temp.close()

            effective_end_dt = (
                latest_data_ts if latest_data_ts else datetime.now(timezone.utc)
            )
            effective_start_dt = effective_end_dt - current_view_info.get(
                "default_lookback", timedelta(days=30)
            )
            start_time_for_input = effective_start_dt.strftime("%Y-%m-%dT%H:%M")
            end_time_for_input = effective_end_dt.strftime("%Y-%m-%dT%H:%M")

    if selected_symbol_id:
        kline_stats = get_kline_stats_for_symbol(selected_symbol_id)
        conn = get_db_connection()
        if conn:
            query_start_time_dt, query_end_time_dt = None, None
            try:
                query_start_time_dt = datetime.fromisoformat(
                    start_time_for_input
                ).replace(tzinfo=timezone.utc)
                query_end_time_dt = datetime.fromisoformat(end_time_for_input).replace(
                    tzinfo=timezone.utc
                )
                if query_start_time_dt >= query_end_time_dt:
                    flash("Start time must be before end time.", "warning")
                    query_error = "Start time must be before end time."
            except ValueError:
                flash("Invalid date format provided. Use YYYY-MM-DDTHH:MM.", "warning")
                query_error = "Invalid date format."

            if not query_error:
                try:
                    table_or_view_name = current_view_info["view_name"]
                    time_column = current_view_info["time_column"]
                    trades_col = current_view_info["trades_column"]
                    open_col, high_col, low_col, close_col = (
                        ("open_price", "high_price", "low_price", "close_price")
                        if not current_view_info["is_aggregate"]
                        else ("open", "high", "low", "close")
                    )
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT EXISTS (
                                SELECT 1 FROM pg_class WHERE relname = %s AND relkind IN ('r', 'p', 'v', 'm', 'f')
                            );
                        """,
                            (table_or_view_name,),
                        )
                        if not cur.fetchone()[0]:
                            query_error = (
                                f"Data source '{table_or_view_name}' not found."
                            )
                            flash(query_error, "warning")
                            klines_data = []
                        else:
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
                            sql_query = f"""
                                SELECT {time_column} AS time, {open_col} AS open_price, {high_col} AS high_price, 
                                       {low_col} AS low_price, {close_col} AS close_price, volume, {trades_col} AS number_of_trades 
                                FROM {table_or_view_name} 
                                WHERE symbol_id = %s {interval_filter_sql} AND {time_column} >= %s AND {time_column} <= %s 
                                ORDER BY {time_column} DESC LIMIT %s """
                            cur.execute(sql_query, tuple(sql_params))
                            klines_data = cur.fetchall()
                    if klines_data:
                        chart_labels = [
                            kr["time"].isoformat() for kr in reversed(klines_data)
                        ]
                        chart_close_prices = [
                            float(kr["close_price"]) for kr in reversed(klines_data)
                        ]
                        chart_volumes = [
                            float(kr["volume"]) for kr in reversed(klines_data)
                        ]
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
            if conn:
                conn.close()
        else:
            query_error = "Database connection failed."
            flash(query_error, "danger")
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


# --- /order-book route ---
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
                        """SELECT obs.time, obs.last_update_id, obs.bids, obs.asks, s.instrument_name 
                           FROM order_book_snapshots obs JOIN symbols s ON obs.symbol_id = s.symbol_id 
                           WHERE obs.symbol_id = %s ORDER BY obs.time DESC LIMIT %s""",
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
                            except (IndexError, TypeError, ValueError) as sort_e:
                                app.logger.error(
                                    f"Could not sort bids/asks for snapshot LUID {snapshot_item.get('last_update_id', 'N/A')}: {sort_e}"
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
            flash(query_error, "danger")
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
    try:
        webui_port = int(config.get("webui", "port", fallback=5001))
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
        webui_port = 5001
    app.run(debug=True, host="0.0.0.0", port=webui_port)
