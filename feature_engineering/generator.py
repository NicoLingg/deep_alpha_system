import argparse
import pandas as pd
import numpy as np
from psycopg2.extras import execute_values, RealDictCursor
from datetime import datetime, timezone, timedelta
import importlib
from sqlalchemy import text
import traceback
import os

from .calculators.catalog import BACKWARD_CALCULATOR_CATALOG, FORWARD_CALCULATOR_CATALOG

from .utils import (
    load_feature_set_config,
    get_kline_source_info,
    parse_interval_to_pandas_offset,
)

# TODO: refactor to common utils
try:
    from data_ingestion.utils import (
        get_db_connection,
        get_sqlalchemy_engine,
        load_config as load_app_config,
    )
except ImportError:
    print("CRITICAL ERROR: Could not import from data_ingestion.utils.")


def _dynamic_import_class(class_path_str: str):
    module_name, class_name = class_path_str.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        print(
            f"ERROR: Could not import class '{class_name}' from '{module_name}'. Path: '{class_path_str}'. Error: {e}"
        )
        raise


def fetch_base_klines(
    sa_engine, symbol_id, interval_str, start_date_dt=None, end_date_dt=None
):
    source_info = get_kline_source_info(interval_str)
    params_dict = {"symbol_id_val": symbol_id}
    interval_filter_sql = ""
    if source_info["table_name"] == "klines":
        interval_filter_sql = "AND interval = :interval_val"
        params_dict["interval_val"] = interval_str

    time_col_name = f"\"{source_info['time_col']}\""
    date_filter_sql = ""
    if start_date_dt:
        date_filter_sql += f" AND {time_col_name} >= :start_date_val"
        params_dict["start_date_val"] = start_date_dt
    if end_date_dt:
        date_filter_sql += f" AND {time_col_name} <= :end_date_val"
        params_dict["end_date_val"] = end_date_dt

    open_col_name = f"\"{source_info['open_col']}\""
    high_col_name = f"\"{source_info['high_col']}\""
    low_col_name = f"\"{source_info['low_col']}\""
    close_col_name = f"\"{source_info['close_col']}\""
    volume_col_name = f"\"{source_info['volume_col']}\""
    qvol_col_name = source_info.get("quote_volume_col")
    qvol_sql = f'"{qvol_col_name}"' if qvol_col_name else "0.0::DECIMAL"

    query_str = f"""SELECT {time_col_name} AS time, symbol_id, {open_col_name} AS open_price, 
                    {high_col_name} AS high_price, {low_col_name} AS low_price, {close_col_name} AS close_price,
                    {volume_col_name} AS volume, {qvol_sql} AS quote_asset_volume
                    FROM "{source_info['table_name']}" WHERE symbol_id = :symbol_id_val {interval_filter_sql} {date_filter_sql}
                    ORDER BY {time_col_name} ASC;"""
    query = text(query_str)
    try:
        df = pd.read_sql_query(query, sa_engine, params=params_dict)
    except Exception as e:
        print(
            f"Error fetching klines for {symbol_id} interval {interval_str}: {e}\nQuery: {query_str}\nParams: {params_dict}"
        )
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    for col in [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_asset_volume",
    ]:
        df[col] = pd.to_numeric(
            df.get(col), errors="coerce"
        )  # Use .get for safety if column missing
    df["time"] = pd.to_datetime(df["time"], utc=True)
    return df.set_index("time", drop=True)


def get_feature_column_definitions_from_config(
    feature_set_config: dict,
):
    cols_set = set()

    def add_col(name, dtype="DECIMAL"):
        name = name.lower().replace("-", "_").replace(".", "_")
        if len(name) > 63:
            original_name = name
            name = name[:60] + "_tr"
            print(f"Warning: Col name '{original_name}' truncated to '{name}'.")
        cols_set.add(f"{name} {dtype}")

    # Process Backward Features from YAML config
    if "backward_features" in feature_set_config:
        for block_name, selection_config in feature_set_config[
            "backward_features"
        ].items():
            calculator_name = selection_config.get("calculator")
            if (
                not calculator_name
                or calculator_name not in BACKWARD_CALCULATOR_CATALOG
            ):
                print(
                    f"ERROR: Invalid or missing 'calculator': '{calculator_name}' in YAML block '{block_name}'. Skipping schema."
                )
                continue

            catalog_entry = BACKWARD_CALCULATOR_CATALOG[calculator_name]
            # Parameters for calculator: start with an empty dict, then apply YAML params
            # The calculator's __init__ should handle its own defaults if a param is missing.
            final_params = selection_config.get("params", {}).copy()

            output_prefix = selection_config.get(
                "output_prefix", block_name
            )  # Use block_name as default prefix
            calc_class_path = catalog_entry["class_path"]

            try:
                CalculatorClass = _dynamic_import_class(calc_class_path)
                # Pass only params from YAML. Calculator __init__ uses its defaults for missing ones.
                calc_instance = CalculatorClass(final_params)
                column_specs = calc_instance.get_output_column_specs()
                for col_suffix, col_dtype in column_specs:
                    add_col(f"{output_prefix}_base_{col_suffix}", col_dtype)
            except Exception as e:
                print(
                    f"ERROR processing schema for backward calculator '{calculator_name}' (class '{calc_class_path}') for block '{block_name}': {e}"
                )
                traceback.print_exc()

    # Process Forward Features from YAML config
    if "forward_features" in feature_set_config:
        for base_interval_str, setup_for_interval in feature_set_config[
            "forward_features"
        ].items():
            if not setup_for_interval:
                continue
            for horizon_str in setup_for_interval.get("horizons", []):
                sane_horizon_str = horizon_str.replace(" ", "").lower()
                try:
                    horizon_timedelta = parse_interval_to_pandas_offset(
                        horizon_str
                    )  # Use imported/local function
                except Exception as e_parse:
                    print(
                        f"ERROR parsing horizon '{horizon_str}' for schema: {e_parse}. Skipping."
                    )
                    continue

                for metric_selection in setup_for_interval.get("metrics", []):
                    calculator_name = metric_selection.get("calculator")
                    if (
                        not calculator_name
                        or calculator_name not in FORWARD_CALCULATOR_CATALOG
                    ):
                        print(
                            f"ERROR: Invalid or missing 'calculator': '{calculator_name}' for forward metric. Skipping schema."
                        )
                        continue

                    catalog_entry = FORWARD_CALCULATOR_CATALOG[calculator_name]
                    final_metric_params = metric_selection.get("params", {}).copy()

                    # Determine metric_type for constructor (used by calculator for its suffix)
                    # Priority: YAML metric_selection -> catalog entry -> calculator_name (as fallback)
                    metric_type_for_calc = metric_selection.get(
                        "metric_type",
                        catalog_entry.get(
                            "default_metric_type", calculator_name.lower()
                        ),
                    )
                    final_metric_params["type"] = (
                        metric_type_for_calc  # Ensure 'type' is in params for calculator
                    )

                    calc_class_path = catalog_entry["class_path"]
                    try:
                        ForwardCalcClass = _dynamic_import_class(calc_class_path)
                        forward_calc_instance = ForwardCalcClass(
                            metric_specific_params=final_metric_params,
                            horizon_timedelta=horizon_timedelta,
                            horizon_str=sane_horizon_str,
                        )
                        metric_suffix_part, col_dtype = (
                            forward_calc_instance.get_output_column_spec()
                        )
                        add_col(f"fwd_{metric_suffix_part}", col_dtype)
                    except Exception as e:
                        print(
                            f"ERROR processing schema for forward calculator '{calculator_name}' (class '{calc_class_path}') for horizon '{sane_horizon_str}': {e}"
                        )
                        traceback.print_exc()

    current_col_names_in_set = {c.split(" ")[0] for c in cols_set}
    if "volume_base" not in current_col_names_in_set:
        add_col("volume_base")
    if "quote_asset_volume_base" not in current_col_names_in_set:
        add_col("quote_asset_volume_base")

    return sorted(list(cols_set))


def ensure_feature_table_exists(db_conn_raw, table_name, feature_set_config: dict):
    with db_conn_raw.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = %s);",
            (table_name,),
        )
        table_exists = cur.fetchone()["exists"]
        if not table_exists:
            print(f"Table {table_name} does not exist. Creating now...")
            feature_col_defs_list = get_feature_column_definitions_from_config(
                feature_set_config
            )  # Pass parsed YAML
            columns_sql_segment = (
                ",\n    " + ",\n    ".join(feature_col_defs_list)
                if feature_col_defs_list
                else ""
            )
            create_table_sql = f"""CREATE TABLE "{table_name}" (
                time TIMESTAMPTZ NOT NULL, symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id),
                base_interval VARCHAR(10) NOT NULL {columns_sql_segment}, 
                ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (time, symbol_id, base_interval));"""
            print(f"Executing CREATE TABLE SQL for {table_name}:\n{create_table_sql}")
            cur.execute(create_table_sql)
            cur.execute(
                f"SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 month');"
            )
            safe_name = "".join(filter(str.isalnum, table_name))[
                : 63 - len("idx__sym_bint_time")
            ]
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{safe_name}_sym_bint_time" ON "{table_name}" (symbol_id, base_interval, time DESC);'
            )
            db_conn_raw.commit()
            print(f'Table "{table_name}" created, hypertable enabled, index created.')
        else:
            print(f'Table "{table_name}" already exists.')


def calculate_features_on_base_df(
    df_base_klines: pd.DataFrame, base_interval_str: str, feature_set_config: dict
):
    if df_base_klines.empty:
        return pd.DataFrame()
    df_base_klines = df_base_klines.sort_index()
    features_df = pd.DataFrame(index=df_base_klines.index)
    features_df["time"] = df_base_klines.index
    features_df["symbol_id"] = (
        df_base_klines["symbol_id"].iloc[0]
        if not df_base_klines.empty and "symbol_id" in df_base_klines.columns
        else None
    )
    features_df["base_interval"] = base_interval_str

    for col in [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_asset_volume",
    ]:
        if col in df_base_klines.columns:
            features_df[col] = df_base_klines[col]
    if "volume" in df_base_klines.columns:
        features_df["volume_base"] = df_base_klines["volume"]
    if "quote_asset_volume" in df_base_klines.columns:
        features_df["quote_asset_volume_base"] = df_base_klines["quote_asset_volume"]

    # Process Backward Features
    if "backward_features" in feature_set_config:
        for block_name, selection_config in feature_set_config[
            "backward_features"
        ].items():
            calculator_name = selection_config.get("calculator")
            if (
                not calculator_name
                or calculator_name not in BACKWARD_CALCULATOR_CATALOG
            ):
                print(
                    f"  ERROR: Invalid 'calculator': '{calculator_name}' in YAML '{block_name}'. Skipping calc."
                )
                continue

            catalog_entry = BACKWARD_CALCULATOR_CATALOG[calculator_name]
            final_params = selection_config.get("params", {}).copy()
            output_prefix = selection_config.get("output_prefix", block_name)
            calc_class_path = catalog_entry["class_path"]
            try:
                CalculatorClass = _dynamic_import_class(calc_class_path)
                calc_instance = CalculatorClass(final_params)
                feature_outputs_dict = calc_instance.calculate(features_df.copy())
                for suffix, series_data in feature_outputs_dict.items():
                    col_name = (
                        f"{output_prefix}_base_{suffix.strip('_')}".lower()
                        .replace("-", "_")
                        .replace(".", "_")
                    )
                    if len(col_name) > 63:
                        col_name = col_name[:60] + "_tr"
                    features_df[col_name] = series_data
            except Exception as e:
                print(
                    f"  ERROR calculating backward calculator '{calculator_name}' for block '{block_name}': {e}"
                )
                traceback.print_exc()

    # Process Forward Features
    if (
        "forward_features" in feature_set_config
        and base_interval_str in feature_set_config["forward_features"]
    ):
        setup_for_interval = feature_set_config["forward_features"][base_interval_str]
        if setup_for_interval:
            lookup_df_klines = df_base_klines.reset_index().sort_values("time")
            for horizon_str_cfg in setup_for_interval.get("horizons", []):
                sane_horizon = horizon_str_cfg.replace(" ", "").lower()
                try:
                    horizon_td = parse_interval_to_pandas_offset(
                        horizon_str_cfg
                    )  # Use imported/local function
                except Exception as e_parse:
                    print(
                        f"    Skipping invalid horizon '{horizon_str_cfg}': {e_parse}"
                    )
                    continue

                for metric_selection in setup_for_interval.get("metrics", []):
                    calculator_name = metric_selection.get("calculator")
                    if (
                        not calculator_name
                        or calculator_name not in FORWARD_CALCULATOR_CATALOG
                    ):
                        print(
                            f"  ERROR: Invalid 'calculator': '{calculator_name}' for forward metric. Skipping calc."
                        )
                        continue

                    catalog_entry = FORWARD_CALCULATOR_CATALOG[calculator_name]
                    final_metric_params = metric_selection.get("params", {}).copy()
                    metric_type_for_calc = metric_selection.get(
                        "metric_type",
                        catalog_entry.get(
                            "default_metric_type", calculator_name.lower()
                        ),
                    )
                    final_metric_params["type"] = metric_type_for_calc
                    calc_class_path = catalog_entry["class_path"]
                    try:
                        ForwardCalcClass = _dynamic_import_class(calc_class_path)
                        fwd_calc = ForwardCalcClass(
                            final_metric_params, horizon_td, sane_horizon
                        )
                        col_suffix_part, _ = fwd_calc.get_output_column_spec()
                        final_col_name = (
                            f"fwd_{col_suffix_part}".lower()
                            .replace("-", "_")
                            .replace(".", "_")
                        )
                        if len(final_col_name) > 63:
                            final_col_name = final_col_name[:60] + "_tr"
                        values = [
                            fwd_calc.calculate_for_timestamp(lookup_df_klines, ts)
                            for ts in features_df["time"]
                        ]
                        features_df[final_col_name] = pd.Series(
                            values, index=features_df.index
                        )
                    except Exception as e:
                        print(
                            f"  ERROR calculating forward calculator '{calculator_name}' for horizon '{sane_horizon}': {e}"
                        )
                        traceback.print_exc()
    return features_df.reset_index(drop=True)


def store_features_batch(db_conn_raw, features_df, table_name):
    if features_df.empty:
        return 0
    with db_conn_raw.cursor() as cur:
        cur.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = %s AND table_schema = 'public' ORDER BY ordinal_position;",
            (table_name,),
        )
        db_cols = [row["column_name"] for row in cur.fetchall()]

    cols_to_insert = [
        col for col in db_cols if col in features_df.columns and col != "ingested_at"
    ]
    if not cols_to_insert:
        print(
            f"    Warning: No matching columns to insert for table {table_name}. DF cols: {features_df.columns.tolist()}"
        )
        return 0

    df_storage = features_df[cols_to_insert].copy().replace([np.inf, -np.inf], np.nan)
    records = [
        tuple(None if pd.isna(v) else v for v in row)
        for _, row in df_storage.iterrows()
    ]
    if not records:
        return 0

    cols_sql = ", ".join(f'"{c}"' for c in cols_to_insert)
    update_cols = [
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols_to_insert
        if c not in ["time", "symbol_id", "base_interval"]
    ]
    conflict_sql = (
        f"DO UPDATE SET {', '.join(update_cols)}, ingested_at = CURRENT_TIMESTAMP"
        if update_cols
        else "DO NOTHING"
    )
    query = f'INSERT INTO "{table_name}" ({cols_sql}) VALUES %s ON CONFLICT (time, symbol_id, base_interval) {conflict_sql};'

    with db_conn_raw.cursor() as cursor:
        try:
            execute_values(cursor, query, records, page_size=500)
            db_conn_raw.commit()
            return len(records)
        except Exception as e:
            db_conn_raw.rollback()
            print(
                f"    DB/GENERAL ERROR storing features into {table_name}: {e}\nQuery (first 200): {query[:200]}..."
            )
            return 0


def main():
    parser = argparse.ArgumentParser(description="Feature Engineering Generator")
    parser.add_argument("--app-config", type=str, default="config/config.ini")
    parser.add_argument(
        "--feature-set-version",
        type=str,
        required=True,
        help="Unique name for this feature set's DB table.",
    )
    parser.add_argument(
        "--feature-set-file",
        type=str,
        help="Name or path of the YAML feature set file (e.g., 'default' or 'feature_sets/custom.yaml'). Defaults to 'feature_sets/default.yaml'.",
    )
    parser.add_argument("--symbols", type=str)
    parser.add_argument("--intervals", type=str)
    parser.add_argument("--start-date", type=str)
    parser.add_argument("--end-date", type=str)
    parser.add_argument("--recalculate-all", action="store_true")
    parser.add_argument("--lookback-days-initial", type=int, default=365)
    parser.add_argument("--kline-buffer-backward-periods", type=int, default=250)
    parser.add_argument("--max-future-horizon-buffer-periods", type=int, default=10)
    args = parser.parse_args()

    # Load feature set configuration from YAML
    # If args.feature_set_file is None, load_feature_set_config will try 'default.yaml'
    feature_set_config_data = load_feature_set_config(args.feature_set_file)
    if not feature_set_config_data:
        print("CRITICAL: Could not load any feature set configuration. Exiting.")
        return

    sane_version_name = "".join(
        c if c.isalnum() else "_" for c in args.feature_set_version
    ).lower()
    feature_table_name = f"kline_features_{sane_version_name}"
    print(
        f"Target Feature Set Version (for table): '{args.feature_set_version}', DB Table: '{feature_table_name}'"
    )
    if args.feature_set_file:
        print(f"Using Feature Set Definition File: '{args.feature_set_file}'")
    else:
        default_path = os.path.join(
            os.path.dirname(__file__), "feature_sets", "default.yaml"
        )
        print(
            f"Using Default Feature Set Definition (implicitly from: {default_path} or similar)"
        )

    app_cfg = load_app_config(args.app_config)
    sa_engine, db_conn_raw = None, None
    try:
        sa_engine = get_sqlalchemy_engine(app_cfg, new_instance=True)
        db_conn_raw = get_db_connection(
            app_cfg, new_instance=True, cursor_factory=RealDictCursor
        )
        print("DB resources initialized.")
        ensure_feature_table_exists(
            db_conn_raw, feature_table_name, feature_set_config_data
        )

        with db_conn_raw.cursor() as cur:
            s_list = (
                [s.strip().upper() for s in args.symbols.split(",")]
                if args.symbols
                else []
            )
            query_params = (s_list,) if s_list else None
            sql_query = f"SELECT symbol_id, instrument_name FROM symbols {'WHERE instrument_name = ANY(%s)' if s_list else ''} ORDER BY instrument_name"
            cur.execute(sql_query, query_params)
            target_symbols_map = {
                r["instrument_name"]: r["symbol_id"] for r in cur.fetchall()
            }
        if not target_symbols_map:
            print("No symbols to process.")
            return

        # Intervals: CLI override > YAML default > hardcoded fallback (though YAML should have it)
        target_intervals = (
            [i.strip() for i in args.intervals.split(",")]
            if args.intervals
            else feature_set_config_data.get(
                "default_base_intervals_to_process", ["1m"]
            )
        )

        for instrument_name, symbol_id in target_symbols_map.items():
            print(f"\nProcessing Symbol: {instrument_name} (ID: {symbol_id})")
            for base_interval_str in target_intervals:
                print(f"  Base Interval: {base_interval_str}")
                calc_end_dt = (
                    pd.to_datetime(args.end_date, utc=True, errors="coerce")
                    if args.end_date
                    else datetime.now(timezone.utc).replace(microsecond=0)
                )
                calc_start_dt = (
                    pd.to_datetime(args.start_date, utc=True, errors="coerce")
                    if args.start_date
                    else None
                )

                if not args.recalculate_all and not calc_start_dt:
                    with db_conn_raw.cursor() as cur:
                        cur.execute(
                            f'SELECT MAX(time) as mt FROM "{feature_table_name}" WHERE symbol_id = %s AND base_interval = %s',
                            (symbol_id, base_interval_str),
                        )
                        last_ts_row = cur.fetchone()
                        last_ts = (
                            last_ts_row["mt"]
                            if last_ts_row and last_ts_row["mt"]
                            else None
                        )
                        if last_ts:
                            calc_start_dt = last_ts + timedelta(seconds=1)
                            print(
                                f"    Resuming from: {calc_start_dt:%Y-%m-%d %H:%M:%S UTC}"
                            )
                if not calc_start_dt:
                    calc_start_dt = calc_end_dt - pd.Timedelta(
                        days=args.lookback_days_initial
                    )
                    print(f"    Initial start: {calc_start_dt:%Y-%m-%d %H:%M:%S UTC}")
                if calc_start_dt >= calc_end_dt:
                    print(f"    Start >= End. Skipping.")
                    continue

                try:
                    td_interval = parse_interval_to_pandas_offset(
                        base_interval_str
                    )  # Use imported/local function
                except Exception as e:
                    print(
                        f"    ERROR parsing interval '{base_interval_str}': {e}. Skipping."
                    )
                    continue

                kl_fetch_start = calc_start_dt - (
                    td_interval * args.kline_buffer_backward_periods
                )
                max_fwd_td = pd.Timedelta(0)
                if (
                    "forward_features" in feature_set_config_data
                    and base_interval_str in feature_set_config_data["forward_features"]
                ):
                    setup = feature_set_config_data["forward_features"][
                        base_interval_str
                    ]
                    for h_str in setup.get("horizons", []):
                        try:
                            max_fwd_td = max(
                                max_fwd_td, parse_interval_to_pandas_offset(h_str)
                            )
                        except:
                            pass  # Ignore parsing errors for buffer calculation

                kl_fetch_end = (
                    calc_end_dt
                    + max_fwd_td
                    + (td_interval * args.max_future_horizon_buffer_periods)
                )
                print(
                    f"    Calc range:  [{calc_start_dt:%F %T}] to [{calc_end_dt:%F %T}] UTC"
                )
                print(
                    f"    Kline fetch: [{kl_fetch_start:%F %T}] to [{kl_fetch_end:%F %T}] UTC"
                )

                try:
                    df_klines = fetch_base_klines(
                        sa_engine,
                        symbol_id,
                        base_interval_str,
                        kl_fetch_start,
                        kl_fetch_end,
                    )
                    if df_klines.empty or len(df_klines) < (
                        args.kline_buffer_backward_periods / 4
                    ):
                        print(f"    Insufficient klines ({len(df_klines)}). Skipping.")
                        continue
                    print(f"    Fetched {len(df_klines)} klines.")

                    features_df = calculate_features_on_base_df(
                        df_klines.copy(), base_interval_str, feature_set_config_data
                    )

                    features_df["time"] = pd.to_datetime(features_df["time"])
                    df_to_store = features_df[
                        (features_df["time"] >= calc_start_dt)
                        & (features_df["time"] <= calc_end_dt)
                    ].copy()

                    if df_to_store.empty:
                        print(f"    No features in target range. Skipping storage.")
                        continue
                    print(
                        f"    Calculated {len(df_to_store)} feature rows for storage."
                    )
                    stored_count = store_features_batch(
                        db_conn_raw, df_to_store, feature_table_name
                    )
                    print(
                        f'    Stored/Updated {stored_count} rows in "{feature_table_name}".'
                    )
                except Exception as e_calc:
                    print(
                        f"    ERROR during calc/store for {instrument_name}-{base_interval_str}: {e_calc}"
                    )
                    traceback.print_exc()
    except Exception as e_outer:
        print(f"Outer script ERROR: {e_outer}")
        traceback.print_exc()
    finally:
        if sa_engine:
            sa_engine.dispose()
            print("SQLAlchemy engine disposed.")
        if db_conn_raw:
            db_conn_raw.close()
            print("Raw DB connection closed.")
        print("Feature generation script finished.")


if __name__ == "__main__":
    main()
