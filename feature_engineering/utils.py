import pandas as pd
import yaml
import os

# TODO: refactor to common utils
try:
    from web_ui.webui import AGGREGATED_INTERVALS_MAP
except ImportError as e_agg_map:
    print(
        f"Warning: Could not import AGGREGATED_INTERVALS_MAP from web_ui.webui: {e_agg_map}. Using fallback."
    )


def parse_interval_to_pandas_offset(interval_str: str) -> pd.Timedelta:
    """
    Converts an interval string (e.g., '1m', '5h', '2d', '1w') to a Pandas Timedelta object.
    """
    unit_map = {"m": "min", "h": "h", "d": "D", "w": "W"}
    num_part_str = "".join(filter(str.isdigit, interval_str))
    unit_part_str = "".join(filter(str.isalpha, interval_str))
    if not num_part_str or not unit_part_str:
        raise ValueError(f"Invalid interval string format: {interval_str}.")
    unit_key = unit_part_str.lower()
    if unit_key not in unit_map:
        raise ValueError(
            f"Invalid interval unit: '{unit_part_str}'. Supported: m, h, d, w."
        )
    num_part = int(num_part_str)
    return pd.Timedelta(f"{num_part}{unit_map[unit_key]}")


def load_feature_set_config(config_name_or_path: str | None = None) -> dict | None:
    """
    Loads a feature set configuration from a YAML file.
    - If config_name_or_path is None, attempts to load 'default.yaml' from a 'feature_sets' subdirectory.
    - If it's a name (e.g., 'my_set'), attempts to load '<name>.yaml' from 'feature_sets'.
    - If it's a full path, attempts to load that path.
    """
    base_config_dir = os.path.join(os.path.dirname(__file__), "feature_sets")

    if config_name_or_path is None:
        config_file_path = os.path.join(base_config_dir, "default.yaml")
        print(f"Attempting to load default feature set: {config_file_path}")
    elif os.path.isfile(config_name_or_path):  # It's a full path
        config_file_path = config_name_or_path
        print(f"Attempting to load feature set from path: {config_file_path}")
    else:
        config_file_name = (
            f"{config_name_or_path}.yaml"
            if not config_name_or_path.endswith(".yaml")
            else config_name_or_path
        )
        config_file_path = os.path.join(base_config_dir, config_file_name)
        print(f"Attempting to load feature set: {config_file_path}")

    if not os.path.exists(config_file_path):
        print(f"ERROR: Feature set configuration file not found: {config_file_path}")
        return None

    try:
        with open(config_file_path, "r") as f:
            config_data = yaml.safe_load(f)
        print(f"Successfully loaded feature set configuration from: {config_file_path}")
        return config_data
    except yaml.YAMLError as e:
        print(f"ERROR parsing YAML file {config_file_path}: {e}")
    except Exception as e:
        print(f"ERROR loading feature set configuration {config_file_path}: {e}")
    return None


def get_kline_source_info(interval_str: str) -> dict:
    if not isinstance(interval_str, str):
        raise ValueError(f"interval_str must be a string, got {type(interval_str)}")

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
                f"Interval {interval_str} mapped but not marked as aggregate."
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
            ),
            "is_raw": False,
        }
    else:
        print(
            f"Warning: Interval '{interval_str}' not in AGGREGATED_INTERVALS_MAP. Assuming raw 'klines' structure."
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
