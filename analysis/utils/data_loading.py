import pandas as pd
import os
import sys
import traceback
from psycopg2.extras import DictCursor
from sqlalchemy import text, exc as sa_exc

# Ensure project root is in sys.path
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from data_ingestion.utils import (
    get_sqlalchemy_engine,
    load_config as load_app_config,
    get_db_connection as get_raw_psycopg2_connection,
)
from web_ui.webui import AGGREGATED_INTERVALS_MAP

_sa_engine_analysis = None
_app_cfg_analysis = None


def _initialize_sa_engine(app_config_path=None):
    """Initializes global SQLAlchemy engine for analysis module."""
    global _sa_engine_analysis, _app_cfg_analysis

    resolved_config_path = app_config_path
    if resolved_config_path is None:
        resolved_config_path = os.path.join(_project_root, "config", "config.ini")

    if _app_cfg_analysis is None:
        _app_cfg_analysis = load_app_config(resolved_config_path)

    if _sa_engine_analysis is None:
        if _app_cfg_analysis:
            print("Initializing SQLAlchemy engine for analysis utilities...")
            _sa_engine_analysis = get_sqlalchemy_engine(
                _app_cfg_analysis, new_instance=True
            )
            if _sa_engine_analysis:
                print("SQLAlchemy engine initialized successfully for analysis.")
            else:
                raise ConnectionError(
                    "Failed to initialize SQLAlchemy engine for analysis."
                )
        else:
            raise RuntimeError(
                "App config not loaded. Cannot init SQLAlchemy engine for analysis."
            )
    return _sa_engine_analysis


def get_active_sa_engine(app_config_path=None):
    """Returns the active SQLAlchemy engine for analysis, initializing if necessary."""
    if _sa_engine_analysis is None:
        _initialize_sa_engine(app_config_path)
    return _sa_engine_analysis


def dispose_sa_engine_connections():
    """Disposes connections in the SQLAlchemy engine's pool for analysis module."""
    global _sa_engine_analysis
    if _sa_engine_analysis:
        print("Disposing SQLAlchemy engine connections (analysis utilities)...")
        _sa_engine_analysis.dispose()
        _sa_engine_analysis = None
        print("SQLAlchemy engine connections disposed (analysis utilities).")
    else:
        print(
            "SQLAlchemy engine (analysis utilities) was not active or already disposed."
        )
    _app_cfg_analysis = None  # Also reset app_cfg to allow full re-init if needed


def get_symbol_id(symbol_name, db_conn_raw=None):
    """
    Fetches symbol_id for a given instrument_name.
    Uses a raw psycopg2 connection.
    """
    conn_to_use = db_conn_raw
    close_conn_after = False  # Flag to close connection if it was opened here

    if conn_to_use is None:
        global _app_cfg_analysis
        if _app_cfg_analysis is None:  # Ensure config is loaded if needed
            # Default path, consider making this configurable or error if not pre-loaded
            config_path = os.path.join(_project_root, "config", "config.ini")
            _app_cfg_analysis = load_app_config(config_path)

        if _app_cfg_analysis:
            conn_to_use = get_raw_psycopg2_connection(
                _app_cfg_analysis, new_instance=True
            )
            if conn_to_use:
                close_conn_after = True
            else:
                raise ConnectionError(
                    "Failed to establish a temporary DB connection for get_symbol_id."
                )
        else:
            raise RuntimeError(
                "App config not available for DB connection in get_symbol_id."
            )

    if not conn_to_use:
        raise ConnectionError("Cannot get symbol_id: No database connection.")

    try:
        # Use DictCursor for easy column name access
        with conn_to_use.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT symbol_id FROM symbols WHERE instrument_name = %s",
                (symbol_name.upper(),),  # Ensure symbol_name is uppercase for matching
            )
            result = cur.fetchone()
            if result:
                return result["symbol_id"]
            else:
                # It's better to return None or raise a specific "NotFound" error
                # than a generic ValueError if this is a common case.
                print(f"Warning: Symbol '{symbol_name}' not found in the database.")
                return None  # Or raise custom SymbolNotFound error
    except Exception as e:
        print(f"Error in get_symbol_id for '{symbol_name}': {e}")
        raise  # Re-raise the exception to be handled by caller
    finally:
        if close_conn_after and conn_to_use:
            conn_to_use.close()


def _get_kline_source_details(interval_str):
    """Internal helper to get kline source table and column names."""

    if interval_str == "1m":
        return {
            "table_name": "klines",
            "time_col": "time",
            "open_col": "open_price",
            "high_col": "high_price",
            "low_col": "low_price",
            "close_col": "close_price",
            "volume_col": "volume",
            "quote_volume_col": "quote_asset_volume",
            "is_raw": True,
        }
    elif interval_str in AGGREGATED_INTERVALS_MAP:
        cagg_info = AGGREGATED_INTERVALS_MAP[interval_str]
        if not cagg_info.get("is_aggregate", False):
            raise ValueError(
                f"Interval {interval_str} is mapped in AGGREGATED_INTERVALS_MAP but not marked as an aggregate."
            )
        return {
            "table_name": cagg_info["view_name"],
            "time_col": cagg_info["time_column"],
            "open_col": "open",
            "high_col": "high",
            "low_col": "low",
            "close_col": "close",
            "volume_col": "volume",
            "quote_volume_col": cagg_info.get(
                "quote_volume_column", "quote_asset_volume"
            ),  # Use a default if not specified
            "is_raw": False,
        }
    else:
        print(
            f"Warning (data_loading): Interval '{interval_str}' not mapped in AGGREGATED_INTERVALS_MAP. "
            "Assuming raw 'klines' table structure."
        )
        return {
            "table_name": "klines",
            "time_col": "time",
            "open_col": "open_price",
            "high_col": "high_price",
            "low_col": "low_price",
            "close_col": "close_price",
            "volume_col": "volume",
            "quote_volume_col": "quote_asset_volume",
            "is_raw": True,
        }


def fetch_klines_df(
    symbol_name,
    interval_str,
    start_date_str=None,
    end_date_str=None,
    limit_val=None,
    sa_engine=None,
):
    engine = sa_engine or get_active_sa_engine()
    symbol_id_val = get_symbol_id(symbol_name)
    if symbol_id_val is None:
        print(f"Could not fetch klines: symbol_id for {symbol_name} not found.")
        return pd.DataFrame()

    source_info = _get_kline_source_details(interval_str)

    params_dict = {"symbol_id_val": symbol_id_val}
    interval_filter_sql = ""
    # If source_info is from 'klines' table, it needs an interval filter
    if (
        source_info["table_name"] == "klines"
    ):  # This covers "1m" and unmapped intervals.
        interval_filter_sql = "AND interval = :interval_val"
        params_dict["interval_val"] = interval_str

    date_filter_sql = ""
    if start_date_str:
        date_filter_sql += f" AND {source_info['time_col']} >= :start_date_val"
        params_dict["start_date_val"] = pd.to_datetime(start_date_str, utc=True)
    if end_date_str:
        date_filter_sql += f" AND {source_info['time_col']} <= :end_date_val"
        params_dict["end_date_val"] = pd.to_datetime(end_date_str, utc=True)

    limit_sql = ""
    if limit_val is not None:
        limit_sql = "LIMIT :limit_val"
        params_dict["limit_val"] = int(limit_val)

    # Standardize output column names from klines for consistency with feature table expectations
    query_str = f"""
        SELECT 
            {source_info['time_col']} AS time, symbol_id,
            {source_info['open_col']} AS open_price, {source_info['high_col']} AS high_price,
            {source_info['low_col']} AS low_price, {source_info['close_col']} AS close_price,
            {source_info['volume_col']} AS volume, 
            {source_info.get('quote_volume_col', '0.0::DECIMAL')} AS quote_asset_volume
        FROM {source_info['table_name']}
        WHERE symbol_id = :symbol_id_val
          {interval_filter_sql} {date_filter_sql}
        ORDER BY {source_info['time_col']} ASC
        {limit_sql};
    """
    # Column names are aliased to open_price, high_price etc. for consistency.

    print(
        f"Fetching klines from: {source_info['table_name']} for {symbol_name} ({interval_str})"
    )
    df = pd.read_sql_query(text(query_str), engine, params=params_dict)
    if not df.empty:
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df.set_index("time")
    return df


def fetch_features_df(
    feature_set_version,
    symbol_name,
    interval_str,  # This is the base_interval for features
    start_date_str=None,
    end_date_str=None,
    limit_val=None,
    sa_engine=None,
):
    engine = sa_engine or get_active_sa_engine()
    symbol_id_val = get_symbol_id(symbol_name)
    if symbol_id_val is None:
        print(f"Could not fetch features: symbol_id for {symbol_name} not found.")
        return pd.DataFrame()

    table_name_sanitized = (
        feature_set_version.lower().replace("-", "_").replace(".", "_")
    )
    feature_table_name = f"kline_features_{table_name_sanitized}"

    params_dict = {
        "symbol_id_val": symbol_id_val,
        "base_interval_val": interval_str,
    }  # Use base_interval_val
    date_filter_sql = ""
    if start_date_str:
        date_filter_sql += " AND time >= :start_date_val"
        params_dict["start_date_val"] = pd.to_datetime(start_date_str, utc=True)
    if end_date_str:
        date_filter_sql += " AND time <= :end_date_val"
        params_dict["end_date_val"] = pd.to_datetime(end_date_str, utc=True)

    limit_sql = ""
    if limit_val is not None:
        limit_sql = "LIMIT :limit_val"
        params_dict["limit_val"] = int(limit_val)

    # *** CORRECTED QUERY TO USE base_interval ***
    query_str = f"""
        SELECT * FROM "{feature_table_name}"
        WHERE symbol_id = :symbol_id_val AND base_interval = :base_interval_val 
          {date_filter_sql}
        ORDER BY time ASC
        {limit_sql};
    """

    print(
        f'Fetching features from: "{feature_table_name}" for {symbol_name} ({interval_str})'
    )
    try:
        df = pd.read_sql_query(text(query_str), engine, params=params_dict)
    except sa_exc.ProgrammingError as e:
        if "relation" in str(e).lower() and "does not exist" in str(e).lower():
            print(
                f"ERROR: Feature table '\"{feature_table_name}\"' does not exist. "
                "Please check the feature_set_version and ensure features have been generated."
            )
            return pd.DataFrame()
        else:
            # Re-raise other programming errors
            print(f"SQLAlchemy ProgrammingError while fetching features: {e}")
            raise
    except Exception as e_other:
        print(f"Unexpected error while fetching features: {e_other}")
        raise

    if not df.empty:
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df.set_index("time")
    return df


def combine_klines_and_features(klines_df, features_df):
    """
    Combines klines DataFrame with features DataFrame.
    Klines DF should have OHLCV columns named open_price, high_price, low_price, close_price, volume, quote_asset_volume.
    """
    if klines_df.empty:
        print(
            "Warning (combine): Klines DataFrame is empty. Returning features DataFrame or empty."
        )
        return features_df  # Or pd.DataFrame() if features_df might also be empty
    if features_df.empty:
        print(
            "Warning (combine): Features DataFrame is empty. Returning klines DataFrame."
        )
        return klines_df

    # Columns to drop from features_df before join to avoid duplication or conflicts
    # 'symbol_id' is already in klines_df. 'base_interval' is metadata for features.
    # 'time' is the index and will be used for joining.
    cols_to_drop_from_features = ["symbol_id", "base_interval", "ingested_at"]

    # Select only the actual feature columns from features_df
    # Assumes 'time' is index, and other metadata cols are specified above.
    feature_columns_to_join = features_df.drop(
        columns=cols_to_drop_from_features, errors="ignore"
    )

    # Perform the join. klines_df provides the base OHLCV.
    # fetch_klines_df already aliases columns to open_price, high_price, etc.
    # If klines_df used 'open', 'high', etc., we'd need to rename them first or adjust here.
    combined_df = klines_df.join(
        feature_columns_to_join, how="left"
    )  # Use left join to keep all klines

    # Optional: Check for duplicate columns after join (should not happen if cols_to_drop_from_features is correct)
    # if combined_df.columns.has_duplicates:
    #     print("Warning: Duplicate columns found after joining klines and features.")
    #     # Handle duplicates, e.g., by keeping first or logging

    return combined_df


# Example usage for direct testing of this module
if __name__ == "__main__":
    print("Testing analysis.utils.data_loading module...")

    # --- Test Configuration ---
    # Ensure these match existing data in your DB for testing
    test_symbol_main = "BTCUSDT"
    test_interval_main = "1h"
    # Replace with a feature set version that ACTUALLY EXISTS in your database
    test_feature_version_main = "v_test_01"  # <<<< IMPORTANT: ENSURE THIS TABLE EXISTS
    test_start_date = "2023-10-01T00:00:00"
    test_end_date = "2023-10-01T05:00:00"  # Small range for quick test
    test_limit = 5
    # --- End Test Configuration ---

    try:
        engine = get_active_sa_engine()  # Initialize/get engine

        print(
            f"\n1. Fetching klines for {test_symbol_main} interval {test_interval_main}..."
        )
        kl_df_main = fetch_klines_df(
            test_symbol_main,
            test_interval_main,
            start_date_str=test_start_date,
            end_date_str=test_end_date,
            limit_val=test_limit,
            sa_engine=engine,
        )
        if not kl_df_main.empty:
            print("Klines fetched successfully:")
            print(kl_df_main.head())
        else:
            print(
                f"No kline data fetched for {test_symbol_main} {test_interval_main}. Check DB and parameters."
            )

        print(
            f"\n2. Fetching features for {test_symbol_main} interval {test_interval_main} (version: {test_feature_version_main})..."
        )
        feat_df_main = fetch_features_df(
            test_feature_version_main,
            test_symbol_main,
            test_interval_main,  # This is base_interval for features
            start_date_str=test_start_date,
            end_date_str=test_end_date,
            limit_val=test_limit,
            sa_engine=engine,
        )
        if not feat_df_main.empty:
            print("Features fetched successfully:")
            print(feat_df_main.head())
        else:
            print(
                f"No features fetched from kline_features_{test_feature_version_main}. "
                "Ensure the table exists and contains data for the symbol/interval/date range."
            )

        if not kl_df_main.empty and not feat_df_main.empty:
            print("\n3. Combining klines and features...")
            comb_df_main = combine_klines_and_features(kl_df_main, feat_df_main)
            if not comb_df_main.empty:
                print("Combined DataFrame created successfully:")
                print(comb_df_main.head())
                print(f"Shape of combined DataFrame: {comb_df_main.shape}")
            else:
                print(
                    "Combination resulted in an empty DataFrame, though inputs were not empty."
                )
        elif kl_df_main.empty:
            print("\nSkipping combination: Klines DataFrame is empty.")
        elif feat_df_main.empty:
            print("\nSkipping combination: Features DataFrame is empty.")

    except Exception as e_test_main:
        print(f"Error during module test: {type(e_test_main).__name__} - {e_test_main}")
        traceback.print_exc()
    finally:
        dispose_sa_engine_connections()  # Clean up engine connections
