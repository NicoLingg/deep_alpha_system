# Feature Engineering System

This directory contains the core logic for calculating financial features from kline (candlestick) data. The system is designed to be modular, configurable, and extensible.

## System Overview

The feature engineering process involves several key components:

1.  **Kline Data Fetching (`generator.py`):**
    *   Retrieves base kline data (OHLCV) from the database for specified symbols and intervals.
    *   Handles appropriate lookback periods for backward features and lookahead periods for forward features.

2.  **Feature Calculator Classes (`calculators/*.py`):**
    *   These are Python classes responsible for the actual computation of individual features or blocks of related features.
    *   They inherit from Abstract Base Classes (ABCs):
        *   `BackwardFeatureCalculator`: For features calculated using historical data up to the current kline (e.g., moving averages, RSI).
        *   `ForwardFeatureCalculator`: For features calculated based on kline data *after* the current kline (e.g., forward returns, future volatility).
    *   Each calculator class defines:
        *   `__init__(self, params)`: To accept configuration parameters.
        *   `get_output_column_specs(self)` (Backward) or `get_output_column_spec(self)` (Forward): To declare the names (suffixes) and database data types of the columns it will produce.
        *   `calculate(self, df_input_data)` (Backward) or `calculate_for_timestamp(self, df_all_base_klines, current_kline_time)` (Forward): To perform the calculation and return the results.

3.  **Calculator Catalog (`calculators/catalog.py`):**
    *   A Python dictionary that acts as a central registry for all implemented feature calculators.
    *   It maps a user-friendly name (e.g., "EMA", "ForwardLogReturn") to the full Python class path of the calculator and can include a description and default metric type (for forward calculators).
    *   This catalog allows feature sets to be defined by referencing these simple names.

4.  **Feature Set Definition (YAML files in `feature_sets/`):**
    *   Users define which features to compute and how they should be configured using YAML files (e.g., `feature_sets/default.yaml`).
    *   In these YAML files, users:
        *   Select calculators by their name from the `catalog.py`.
        *   Provide specific parameters to override any defaults or tailor the calculation (e.g., periods for an EMA, input columns).
        *   Define unique "block names" for each calculation instance, which also serve as default output prefixes for generated columns.
        *   Specify horizons for forward-looking features.

5.  **Feature Generation Orchestration (`generator.py`):**
    *   The main script that drives the feature calculation process.
    *   It parses command-line arguments (including the feature set version for table naming and the path to the YAML feature set file).
    *   Loads the specified YAML feature set definition.
    *   Uses the Calculator Catalog to find and instantiate the required calculator classes with parameters from the YAML.
    *   Calls `get_output_column_specs()` on these instances to determine the database schema for the feature table.
    *   Creates or ensures the feature table exists in the database (TimescaleDB hypertable).
    *   Iterates through kline data, calling the `calculate()` or `calculate_for_timestamp()` methods of the instantiated calculators.
    *   Merges the results into a comprehensive DataFrame.
    *   Stores the calculated features in the database.

## How to Add a New Feature Calculator

Adding a new feature calculator involves these steps:

**1. Implement the Calculator Class:**

   *   Decide if it's a `BackwardFeatureCalculator` or a `ForwardFeatureCalculator`.
   *   Create a new Python class in an appropriate file within the `feature_engineering/calculators/` directory (e.g., `technical_indicators.py`, `volume_features.py`, or a new file like `my_custom_features.py`).
   *   Your class must inherit from the chosen ABC (`BackwardFeatureCalculator` or `ForwardFeatureCalculator` from `calculators.base_calculator.py`).

   **Example: New Backward Calculator (`MyNewIndicatorCalculator`)**

   ```python
   import pandas as pd
   import numpy as np
   from .base_calculator import BackwardFeatureCalculator

   class MyNewIndicatorCalculator(BackwardFeatureCalculator):
       def __init__(self, params: dict):
           super().__init__(params)
           # Extract and store parameters needed for calculation
           # Example: self.period = params.get("period", 20)
           # Example: self.input_source = params.get("input_source", "close_price")
           # Define any other necessary setup based on params.
           # It's good practice for calculators to have internal defaults
           # if params are not provided in the YAML.

       def get_output_column_specs(self) -> list[tuple[str, str]]:
           # Return a list of (column_suffix, db_dtype)
           # The 'column_suffix' will be appended to the 'output_prefix' from the YAML.
           # Example:
           # period_str = self.params.get("period", "default") # Use param in suffix if it varies
           # return [(f"mynew_{period_str}", "DECIMAL")]
           return [("my_indicator_value", "FLOAT")] # Simple example

       def calculate(self, df_input_data: pd.DataFrame) -> dict[str, pd.Series]:
           results = {}
           # Access input data, e.g., source = df_input_data[self.input_source]
           # Perform your calculation logic...
           # Ensure output Series are indexed like df_input_data.index
           # Example:
           # if self.input_source not in df_input_data.columns:
           #     results["my_indicator_value"] = pd.Series(np.nan, index=df_input_data.index)
           #     return results
           #
           # calculated_series = df_input_data[self.input_source].rolling(window=self.period).mean() # Dummy calc
           # results["my_indicator_value"] = calculated_series
           results["my_indicator_value"] = pd.Series(np.random.rand(len(df_input_data)), index=df_input_data.index) # Placeholder
           return results
   ```

   **Key considerations for your calculator implementation:**
   *   **Parameters (`params`):** Your `__init__` should robustly handle expected parameters. Consider using `params.get("my_param", default_value)` to provide defaults if a parameter is not specified in the YAML.
   *   **Output Column Suffixes:** `get_output_column_specs` must accurately declare the suffixes your `calculate` method will use as keys in its returned dictionary. These suffixes should be unique for the outputs of this specific calculator.
   *   **Data Types:** Choose appropriate database data types (e.g., "DECIMAL", "FLOAT", "INTEGER", "BOOLEAN").
   *   **Input Data (`df_input_data` / `df_all_base_klines`):** Understand the structure of the DataFrame passed to your `calculate` method.
   *   **NaN Handling:** Return `np.nan` for undefined values, not Python `None`. Ensure Series have the same index as the input DataFrame for backward calculators.
   *   **Dependencies:** If your calculator relies on features computed by other calculators, ensure the `input_field` names in its `params` correctly reference the output columns of those prerequisite features (e.g., `ema_close_base_ema_20`).

**2. Register the New Calculator in the Catalog:**

   *   Open `feature_engineering/calculators/catalog.py`.
   *   Add a new entry to either `BACKWARD_CALCULATOR_CATALOG` or `FORWARD_CALCULATOR_CATALOG`.
   *   Choose a simple, user-friendly **name (key)** for your calculator. This is what users will reference in the YAML file.
   *   Provide the full `class_path` to your new calculator class.
   *   Optionally, add a `description`.
   *   For `ForwardFeatureCalculator`s, specify a `default_metric_type` if applicable (this aids in column naming).

   **Example Catalog Entry:**

   ```python
   # In feature_engineering/calculators/catalog.py

   # ... existing entries ...
   "MyNewIndicator": { # User-friendly name for YAML
       "class_path": "feature_engineering.calculators.my_custom_features.MyNewIndicatorCalculator",
       "description": "Calculates a custom indicator based on a rolling window. Params: period, input_source.",
   },
   # ...
   ```

**3. Use the New Calculator in a YAML Feature Set:**

   *   Open an existing YAML file in `feature_sets/` (e.g., `default_v1.yaml`) or create a new one.
   *   Add a new block referencing your calculator by the **name** you gave it in the catalog.

   **Example YAML Usage:**

   ```yaml
   # In feature_sets/my_new_set.yaml
   # ...
   backward_features:
     # ... other features ...
     custom_indicator_on_high:
       calculator: "MyNewIndicator" # Name from catalog.py
       params:
         input_source: "high_price"
         period: 25
       # output_prefix: "custom_high" # Optional, defaults to 'custom_indicator_on_high'
   # ...
   ```

**4. Test:**

   *   Run the feature generation pipeline using the `Makefile` and your new/updated YAML feature set file:
     ```bash
     make calculate-features FEATURE_SET_VERSION=<your_db_table_version_suffix> FEATURE_SET_FILE=<your_yaml_file_name_or_path>
     ```
   *   Verify that the new feature columns are created in the database table with the correct names and data.
   *   Check the logs for any errors during calculation.

## Database Schema and Table Naming

*   The `generator.py` script automatically infers column names and data types by calling `get_output_column_specs()` on each instantiated calculator.
*   Backward feature column names are typically formed as:
    `{output_prefix_from_yaml}_base_{suffix_from_calculator}`
    (e.g., `custom_indicator_on_high_base_my_indicator_value`).
*   Forward feature column names are typically formed as:
    `fwd_{metric_type_from_calculator_spec}_{horizon_from_calculator_spec}`
    (e.g., `fwd_logret_5m`).
*   Feature tables are named `kline_features_<feature_set_version>`, where `<feature_set_version>` is provided via the command line.

## Dependencies Between Features

If one backward feature (e.g., EMA of log returns) depends on another previously calculated backward feature (e.g., log returns), ensure:
1.  The dependent feature block appears *after* its prerequisite in the YAML file (though the current `generator.py` processes `features_df.copy()` so order might not be strictly enforced for calculation but is good for clarity). More importantly:
2.  The `input_field` parameter of the dependent calculator correctly references the fully qualified column name of the prerequisite feature. For example, if a "log_returns" block with `output_prefix: "logret_close"` generates `logret_close_base_log_return_base`, an EMA calculator needing this would specify `input_field: "logret_close_base_log_return_base"` in its YAML `params`.

By following these steps, you can easily extend the system with new and custom feature calculations while keeping the configuration manageable and user-friendly.