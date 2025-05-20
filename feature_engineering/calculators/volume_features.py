import pandas as pd
import numpy as np
import talib
from .base_calculator import BackwardFeatureCalculator


class RollingSumFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_field = self.params.get("input_field")
        self.periods = self.params.get("periods", [])
        self.min_periods_ratio = self.params.get("min_periods_ratio", 0.5)

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        return [
            (f"sum_{p}", "DECIMAL") for p in self.periods
        ]  # Volume sum can be large

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        if self.input_field not in df_input_data.columns:
            for p in self.periods:
                results[f"sum_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        source_series = df_input_data[self.input_field].astype(float)
        if source_series.isnull().all():
            for p in self.periods:
                results[f"sum_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        for p in self.periods:
            min_p_calc = max(1, int(p * self.min_periods_ratio)) if p > 1 else 1
            if len(source_series.dropna()) < min_p_calc:
                results[f"sum_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            else:
                results[f"sum_{p}"] = source_series.rolling(
                    window=p, min_periods=min_p_calc
                ).sum()
        return results


class ObvFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_fields = self.params.get("input_fields", {})
        self.close_field = self.input_fields.get("close", "close_price")
        self.volume_field = self.input_fields.get("volume", "volume")

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        return [("obv", "DECIMAL")]  # OBV can be large, use DECIMAL

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        col_name = "obv"
        if not all(
            f in df_input_data.columns for f in [self.close_field, self.volume_field]
        ):
            results[col_name] = pd.Series(np.nan, index=df_input_data.index)
            return results

        close_series = df_input_data[self.close_field].astype(float)
        volume_series = df_input_data[self.volume_field].astype(float)

        if close_series.isnull().all() or volume_series.isnull().all():
            results[col_name] = pd.Series(np.nan, index=df_input_data.index)
            return results

        if len(close_series.dropna()) < 2 or len(volume_series.dropna()) < 2:
            results[col_name] = pd.Series(np.nan, index=df_input_data.index)
            return results

        obv_values = talib.OBV(close_series.values, volume_series.values)
        results[col_name] = pd.Series(obv_values, index=df_input_data.index)
        return results
