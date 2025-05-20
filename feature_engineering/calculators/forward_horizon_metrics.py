import pandas as pd
import numpy as np
from .base_calculator import ForwardFeatureCalculator


class ForwardLogReturnCalculator(ForwardFeatureCalculator):
    def __init__(
        self,
        metric_specific_params: dict,
        horizon_timedelta: pd.Timedelta,
        horizon_str: str,
    ):
        super().__init__(metric_specific_params, horizon_timedelta, horizon_str)
        self.input_field = self.metric_specific_params.get("input_field", "close_price")
        self.metric_type_name = self.metric_specific_params.get(
            "type", "logret"
        )  # "type" from config

    def get_output_column_spec(self) -> tuple[str, str]:
        suffix = f"{self.metric_type_name}_{self.horizon_str}"
        return (suffix, "DECIMAL")

    def calculate_for_timestamp(
        self, df_all_base_klines: pd.DataFrame, current_kline_time: pd.Timestamp
    ) -> float | None:
        if "time" not in df_all_base_klines.columns:
            return np.nan

        current_kline_rows = df_all_base_klines[
            df_all_base_klines["time"] == current_kline_time
        ]
        if current_kline_rows.empty:
            return np.nan

        current_close = current_kline_rows[self.input_field].iloc[0]
        if pd.isna(current_close) or current_close <= 0:
            return np.nan

        future_timestamp_target = current_kline_time + self.horizon_timedelta
        lookup_df = pd.DataFrame({"lookup_time": [future_timestamp_target]})

        # df_all_base_klines is assumed to be pre-sorted by 'time' by the generator
        relevant_future_klines = df_all_base_klines[["time", self.input_field]]

        future_kline = pd.merge_asof(
            lookup_df,
            relevant_future_klines,
            left_on="lookup_time",
            right_on="time",
            direction="forward",
        )

        if not future_kline.empty and not pd.isna(
            future_kline[self.input_field].iloc[0]
        ):
            future_close = future_kline[self.input_field].iloc[0]
            if future_close <= 0:
                return np.nan
            return np.log(future_close / current_close)
        return np.nan


class ForwardVolatilityCalculator(ForwardFeatureCalculator):
    def __init__(
        self,
        metric_specific_params: dict,
        horizon_timedelta: pd.Timedelta,
        horizon_str: str,
    ):
        super().__init__(metric_specific_params, horizon_timedelta, horizon_str)
        self.input_field = self.metric_specific_params.get("input_field", "close_price")
        self.metric_type_name = self.metric_specific_params.get(
            "type", "vol"
        )  # "type" from config
        self.use_existing_log_returns_col = self.metric_specific_params.get(
            "use_existing_log_returns_col", False
        )
        self.log_return_input_field_for_vol = self.metric_specific_params.get(
            "log_return_input_field_for_vol", "log_return_1p"
        )  # Example

    def get_output_column_spec(self) -> tuple[str, str]:
        suffix = f"{self.metric_type_name}_{self.horizon_str}"
        return (suffix, "DECIMAL")

    def calculate_for_timestamp(
        self, df_all_base_klines: pd.DataFrame, current_kline_time: pd.Timestamp
    ) -> float | None:
        if "time" not in df_all_base_klines.columns:
            return np.nan

        future_end_time_target = current_kline_time + self.horizon_timedelta
        future_klines_segment = df_all_base_klines[
            (df_all_base_klines["time"] > current_kline_time)
            & (df_all_base_klines["time"] <= future_end_time_target)
        ]

        if future_klines_segment.empty or len(future_klines_segment) < 2:
            return np.nan

        if self.input_field not in future_klines_segment.columns:
            return np.nan

        segment_prices = future_klines_segment[self.input_field].astype(float)
        if segment_prices.isnull().all() or (segment_prices <= 0).any():
            return np.nan

        log_returns = pd.Series(dtype=float)
        if (
            self.use_existing_log_returns_col
            and self.log_return_input_field_for_vol in future_klines_segment.columns
        ):
            log_returns = (
                future_klines_segment[self.log_return_input_field_for_vol]
                .astype(float)
                .dropna()
            )
        else:
            shifted_prices = segment_prices.shift(1)
            valid_mask = (segment_prices > 0) & (shifted_prices > 0)
            # Ensure we only calculate log returns where both current and shifted prices are valid
            if valid_mask.any():
                log_returns = np.log(
                    segment_prices[valid_mask] / shifted_prices[valid_mask]
                ).dropna()

        if len(log_returns) < 2:
            return np.nan

        return log_returns.std()
