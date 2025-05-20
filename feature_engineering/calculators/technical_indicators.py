import pandas as pd
import numpy as np
import talib
from .base_calculator import BackwardFeatureCalculator


class EmaFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_field = self.params.get("input_field", "close_price")
        self.periods = self.params.get("periods", [])
        self.min_periods_ratio = self.params.get("min_periods_ratio", 0.5)

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        return [(f"ema_{p}", "DECIMAL") for p in self.periods]

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        if self.input_field not in df_input_data.columns:
            for p in self.periods:
                results[f"ema_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        source_series = df_input_data[self.input_field].astype(float)
        if source_series.isnull().all():
            for p in self.periods:
                results[f"ema_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        for p in self.periods:
            min_p_calc = max(1, int(p * self.min_periods_ratio)) if p > 1 else 1
            if len(source_series.dropna()) < min_p_calc:
                results[f"ema_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            else:
                results[f"ema_{p}"] = source_series.ewm(
                    span=p, adjust=False, min_periods=min_p_calc
                ).mean()
        return results


class RsiFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_field = self.params.get("input_field", "close_price")
        self.talib_periods = self.params.get("talib_periods", [])

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        return [(f"rsi_{p}", "DECIMAL") for p in self.talib_periods]

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        if self.input_field not in df_input_data.columns:
            for p in self.talib_periods:
                results[f"rsi_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        source_series = df_input_data[self.input_field].astype(float)
        if source_series.isnull().all():
            for p in self.talib_periods:
                results[f"rsi_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        for p in self.talib_periods:
            if len(source_series.dropna()) < p:
                results[f"rsi_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            else:
                rsi_values = talib.RSI(source_series.values, timeperiod=p)
                results[f"rsi_{p}"] = pd.Series(rsi_values, index=df_input_data.index)
        return results


class MacdFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_field = self.params.get("input_field", "close_price")
        self.fastperiod = self.params.get("fastperiod", 12)
        self.slowperiod = self.params.get("slowperiod", 26)
        self.signalperiod = self.params.get("signalperiod", 9)

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        fp, sp, sigp = self.fastperiod, self.slowperiod, self.signalperiod
        return [
            (f"macd_{fp}_{sp}_{sigp}", "DECIMAL"),
            (f"macdsignal_{fp}_{sp}_{sigp}", "DECIMAL"),
            (f"macdhist_{fp}_{sp}_{sigp}", "DECIMAL"),
        ]

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        fp, sp, sigp = self.fastperiod, self.slowperiod, self.signalperiod
        suffix_macd = f"macd_{fp}_{sp}_{sigp}"
        suffix_signal = f"macdsignal_{fp}_{sp}_{sigp}"
        suffix_hist = f"macdhist_{fp}_{sp}_{sigp}"

        if self.input_field not in df_input_data.columns:
            results[suffix_macd] = pd.Series(np.nan, index=df_input_data.index)
            results[suffix_signal] = pd.Series(np.nan, index=df_input_data.index)
            results[suffix_hist] = pd.Series(np.nan, index=df_input_data.index)
            return results

        source_series = df_input_data[self.input_field].astype(float)
        if source_series.isnull().all():
            results[suffix_macd] = pd.Series(np.nan, index=df_input_data.index)
            results[suffix_signal] = pd.Series(np.nan, index=df_input_data.index)
            results[suffix_hist] = pd.Series(np.nan, index=df_input_data.index)
            return results

        min_data_points = self.slowperiod + self.signalperiod - 1
        if len(source_series.dropna()) < min_data_points:
            results[suffix_macd] = pd.Series(np.nan, index=df_input_data.index)
            results[suffix_signal] = pd.Series(np.nan, index=df_input_data.index)
            results[suffix_hist] = pd.Series(np.nan, index=df_input_data.index)
            return results

        macd, macdsignal, macdhist = talib.MACD(
            source_series.values,
            fastperiod=self.fastperiod,
            slowperiod=self.slowperiod,
            signalperiod=self.signalperiod,
        )
        results[suffix_macd] = pd.Series(macd, index=df_input_data.index)
        results[suffix_signal] = pd.Series(macdsignal, index=df_input_data.index)
        results[suffix_hist] = pd.Series(macdhist, index=df_input_data.index)
        return results


class AtrFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_fields = self.params.get("input_fields", {})
        self.high_field = self.input_fields.get("high", "high_price")
        self.low_field = self.input_fields.get("low", "low_price")
        self.close_field = self.input_fields.get("close", "close_price")
        self.talib_periods = self.params.get("talib_periods", [])

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        return [(f"atr_{p}", "DECIMAL") for p in self.talib_periods]

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        if not all(
            f in df_input_data.columns
            for f in [self.high_field, self.low_field, self.close_field]
        ):
            for p in self.talib_periods:
                results[f"atr_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        high_series = df_input_data[self.high_field].astype(float)
        low_series = df_input_data[self.low_field].astype(float)
        close_series = df_input_data[self.close_field].astype(float)

        if (
            high_series.isnull().all()
            or low_series.isnull().all()
            or close_series.isnull().all()
        ):
            for p in self.talib_periods:
                results[f"atr_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        for p in self.talib_periods:
            if (
                len(high_series.dropna()) < p
                or len(low_series.dropna()) < p
                or len(close_series.dropna()) < p
            ):
                results[f"atr_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            else:
                atr_values = talib.ATR(
                    high_series.values,
                    low_series.values,
                    close_series.values,
                    timeperiod=p,
                )
                results[f"atr_{p}"] = pd.Series(atr_values, index=df_input_data.index)
        return results


class LogReturnsFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_field = self.params.get("input_field", "close_price")
        self.windows = self.params.get("windows", [])
        self.add_base_return = self.params.get("add_base_return", False)

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        specs = []
        for w in self.windows:
            if w == 0:
                continue
            specs.append((f"log_return_{w}p", "DECIMAL"))
        if self.add_base_return and 1 in self.windows:
            specs.append(("log_return_base", "DECIMAL"))
        return specs

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        if self.input_field not in df_input_data.columns:
            for w in self.windows:
                if w == 0:
                    continue
                results[f"log_return_{w}p"] = pd.Series(
                    np.nan, index=df_input_data.index
                )
            if self.add_base_return and 1 in self.windows:
                results["log_return_base"] = pd.Series(
                    np.nan, index=df_input_data.index
                )
            return results

        source_series = df_input_data[self.input_field].astype(float)
        if source_series.isnull().all():
            for w in self.windows:
                if w == 0:
                    continue
                results[f"log_return_{w}p"] = pd.Series(
                    np.nan, index=df_input_data.index
                )
            if self.add_base_return and 1 in self.windows:
                results["log_return_base"] = pd.Series(
                    np.nan, index=df_input_data.index
                )
            return results

        for window in self.windows:
            if window == 0:
                continue
            col_name = f"log_return_{window}p"
            if len(source_series) > window:
                shifted_series = source_series.shift(window)
                valid_mask = (source_series > 0) & (shifted_series > 0)
                log_return_series = pd.Series(np.nan, index=df_input_data.index)
                log_return_series[valid_mask] = np.log(
                    source_series[valid_mask] / shifted_series[valid_mask]
                )
                results[col_name] = log_return_series
            else:
                results[col_name] = pd.Series(np.nan, index=df_input_data.index)

            if window == 1 and self.add_base_return:
                results["log_return_base"] = results[col_name].copy()
        return results


class RollingStdFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_field = self.params.get("input_field")
        self.periods = self.params.get("periods", [])
        self.min_periods_ratio = self.params.get("min_periods_ratio", 0.5)

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        return [(f"std_{p}", "DECIMAL") for p in self.periods]

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        if self.input_field not in df_input_data.columns:
            for p in self.periods:
                results[f"std_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        source_series = df_input_data[self.input_field].astype(float)
        if source_series.isnull().all():
            for p in self.periods:
                results[f"std_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            return results

        for p in self.periods:
            min_p_calc = max(1, int(p * self.min_periods_ratio)) if p > 1 else 1
            if len(source_series.dropna()) < min_p_calc:
                results[f"std_{p}"] = pd.Series(np.nan, index=df_input_data.index)
            else:
                results[f"std_{p}"] = source_series.rolling(
                    window=p, min_periods=min_p_calc
                ).std()
        return results


class BbandsFeatureCalculator(BackwardFeatureCalculator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.input_field = self.params.get("input_field", "close_price")
        self.timeperiod = self.params.get("timeperiod", 20)
        self.nbdevup = self.params.get("nbdevup", 2)
        self.nbdevdn = self.params.get("nbdevdn", 2)
        self.matype = self.params.get("matype", 0)

    def get_output_column_specs(self) -> list[tuple[str, str]]:
        suffix_detail = f"{self.timeperiod}_{self.nbdevup}"  # nbdevdn usually matches nbdevup for suffix
        return [
            (f"bbands_upper_{suffix_detail}", "DECIMAL"),
            (f"bbands_middle_{suffix_detail}", "DECIMAL"),
            (f"bbands_lower_{suffix_detail}", "DECIMAL"),
        ]

    def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
        results = {}
        suffix_detail = f"{self.timeperiod}_{self.nbdevup}"
        col_upper = f"bbands_upper_{suffix_detail}"
        col_middle = f"bbands_middle_{suffix_detail}"
        col_lower = f"bbands_lower_{suffix_detail}"

        if self.input_field not in df_input_data.columns:
            for col_key in [col_upper, col_middle, col_lower]:
                results[col_key] = pd.Series(np.nan, index=df_input_data.index)
            return results

        source_series = df_input_data[self.input_field].astype(float)
        if source_series.isnull().all():
            for col_key in [col_upper, col_middle, col_lower]:
                results[col_key] = pd.Series(np.nan, index=df_input_data.index)
            return results

        min_data_points = self.timeperiod
        if len(source_series.dropna()) < min_data_points:
            for col_key in [col_upper, col_middle, col_lower]:
                results[col_key] = pd.Series(np.nan, index=df_input_data.index)
            return results

        upper, middle, lower = talib.BBANDS(
            source_series.values,
            timeperiod=self.timeperiod,
            nbdevup=self.nbdevup,
            nbdevdn=self.nbdevdn,
            matype=self.matype,
        )
        results[col_upper] = pd.Series(upper, index=df_input_data.index)
        results[col_middle] = pd.Series(middle, index=df_input_data.index)
        results[col_lower] = pd.Series(lower, index=df_input_data.index)
        return results
