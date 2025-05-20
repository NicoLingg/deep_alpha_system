"""
This module acts as a catalog for all available feature calculators.
It maps user-friendly names to their corresponding class paths and
can provide additional metadata like default metric types for naming.
"""

# User-friendly Name: {
#     "class_path": "full.path.to.CalculatorClass",
#     "description": "Optional human-readable description.",
#     "default_metric_type": "Optional, for forward calculators, aids in naming if not in YAML."
# }

BACKWARD_CALCULATOR_CATALOG = {
    "EMA": {
        "class_path": "feature_engineering.calculators.technical_indicators.EmaFeatureCalculator",
        "description": "Exponential Moving Averages. Params: input_field, periods, min_periods_ratio.",
    },
    "RSI": {
        "class_path": "feature_engineering.calculators.technical_indicators.RsiFeatureCalculator",
        "description": "Relative Strength Index. Params: input_field, talib_periods.",
    },
    "MACD": {
        "class_path": "feature_engineering.calculators.technical_indicators.MacdFeatureCalculator",
        "description": "Moving Average Convergence Divergence. Params: input_field, fastperiod, slowperiod, signalperiod.",
    },
    "ATR": {
        "class_path": "feature_engineering.calculators.technical_indicators.AtrFeatureCalculator",
        "description": "Average True Range. Params: input_fields (high, low, close), talib_periods.",
    },
    "LogReturns": {
        "class_path": "feature_engineering.calculators.technical_indicators.LogReturnsFeatureCalculator",
        "description": "Logarithmic Returns. Params: input_field, windows, add_base_return.",
    },
    "RollingStd": {
        "class_path": "feature_engineering.calculators.technical_indicators.RollingStdFeatureCalculator",
        "description": "Rolling Standard Deviation. Params: input_field, periods, min_periods_ratio.",
    },
    "BBands": {
        "class_path": "feature_engineering.calculators.technical_indicators.BbandsFeatureCalculator",
        "description": "Bollinger Bands. Params: input_field, timeperiod, nbdevup, nbdevdn, matype.",
    },
    "RollingSum": {
        "class_path": "feature_engineering.calculators.volume_features.RollingSumFeatureCalculator",
        "description": "Rolling Sum. Params: input_field, periods, min_periods_ratio.",
    },
    "OBV": {
        "class_path": "feature_engineering.calculators.volume_features.ObvFeatureCalculator",
        "description": "On-Balance Volume. Params: input_fields (close, volume).",
    },
}

FORWARD_CALCULATOR_CATALOG = {
    "ForwardLogReturn": {
        "class_path": "feature_engineering.calculators.forward_horizon_metrics.ForwardLogReturnCalculator",
        "description": "Forward Log Return. Params: input_field. Metric type used for naming.",
        "default_metric_type": "logret",  # This helps in naming: fwd_<metric_type>_<horizon>
    },
    "ForwardVolatility": {
        "class_path": "feature_engineering.calculators.forward_horizon_metrics.ForwardVolatilityCalculator",
        "description": "Forward Volatility of Log Returns. Params: input_field, use_existing_log_returns_col. Metric type used for naming.",
        "default_metric_type": "vol",
    },
}
