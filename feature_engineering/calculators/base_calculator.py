from abc import ABC, abstractmethod
import pandas as pd


class BackwardFeatureCalculator(ABC):
    def __init__(self, params: dict):
        self.params = params

    @abstractmethod
    def get_output_column_specs(self) -> list[tuple[str, str]]:
        """
        Returns a list of tuples: (column_suffix: str, column_db_dtype: str).
        'column_suffix' is the key the 'calculate' method will use in its output dict.
        'column_db_dtype' is a string for the database (e.g., "DECIMAL", "FLOAT", "INTEGER").
        """
        pass

    @abstractmethod
    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        """
        Calculates backward-looking features.
        df_input_data will have a DatetimeIndex and a 'time' column, plus base OHLCV and any prior features.
        Returns a dictionary where keys are 'column_suffix' (matching get_output_column_specs)
        and values are pd.Series indexed like df_input_data.
        """
        pass


class ForwardFeatureCalculator(ABC):
    def __init__(
        self,
        metric_specific_params: dict,
        horizon_timedelta: pd.Timedelta,
        horizon_str: str,
    ):
        """
        metric_specific_params: Parameters specific to the metric (e.g., input_field, type).
        horizon_timedelta: The specific pd.Timedelta for this calculator instance.
        horizon_str: The string representation of the horizon (e.g., "5m", "1h") from the config.
        """
        self.metric_specific_params = metric_specific_params
        self.horizon_timedelta = horizon_timedelta
        self.horizon_str = horizon_str

    @abstractmethod
    def get_output_column_spec(self) -> tuple[str, str]:
        """
        Returns a single tuple: (metric_type_and_horizon_suffix_part: str, column_db_dtype: str).
        The 'metric_type_and_horizon_suffix_part' could be, for example, "logret_5m".
        The generator will prefix this with "fwd_".
        """
        pass

    @abstractmethod
    def calculate_for_timestamp(
        self, df_all_base_klines: pd.DataFrame, current_kline_time: pd.Timestamp
    ) -> float | None:
        """
        Calculates the forward-looking metric for a single timestamp.
        df_all_base_klines has 'time' as a column for lookups.
        current_kline_time is the timestamp for which to calculate the future metric.
        Returns a single scalar value (float, np.nan).
        """
        pass
