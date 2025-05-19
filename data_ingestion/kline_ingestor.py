import time
import psycopg2
import argparse
import pandas as pd
from psycopg2.extras import execute_values
from binance.client import Client


# Use relative import for utils when run as a module (-m)
try:
    from .utils import (
        load_config,
        get_db_connection,
        get_binance_client,
        get_or_create_exchange_id,
        get_or_create_symbol_id,
        DEFAULT_CONFIG_PATH,
    )
except ImportError:
    from utils import (
        load_config,
        get_db_connection,
        get_binance_client,
        get_or_create_exchange_id,
        get_or_create_symbol_id,
        DEFAULT_CONFIG_PATH,
    )


def store_klines_batch(conn, klines_data, symbol_id, interval, batch_size=500):
    if not klines_data:
        return 0
    records_to_insert = []
    for kline in klines_data:
        try:
            records_to_insert.append(
                (
                    pd.to_datetime(kline[0], unit="ms"),
                    symbol_id,
                    interval,
                    float(kline[1]),
                    float(kline[2]),
                    float(kline[3]),
                    float(kline[4]),
                    float(kline[5]),
                    pd.to_datetime(kline[6], unit="ms"),
                    float(kline[7]),
                    int(kline[8]),
                    float(kline[9]),
                    float(kline[10]),
                )
            )
        except (IndexError, ValueError) as e:
            print(f"Warning: Skipping malformed kline data: {kline}. Error: {e}")
            continue  # Skip this kline and proceed with the next

    if not records_to_insert:  # All klines in batch might have been skipped
        return 0

    query = """
        INSERT INTO klines (time, symbol_id, interval, open_price, high_price, low_price, close_price,
                            volume, close_time, quote_asset_volume, number_of_trades,
                            taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
        VALUES %s
        ON CONFLICT (time, symbol_id, interval) DO UPDATE SET
            open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume, close_time = EXCLUDED.close_time,
            quote_asset_volume = EXCLUDED.quote_asset_volume,
            number_of_trades = EXCLUDED.number_of_trades,
            taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
            ingested_at = CURRENT_TIMESTAMP;
    """
    inserted_count = 0
    with conn.cursor() as cursor:
        try:
            execute_values(cursor, query, records_to_insert, page_size=batch_size)
            conn.commit()
            inserted_count = cursor.rowcount
            print(
                f"Successfully processed {inserted_count} klines for symbol_id {symbol_id}, interval {interval} (batch size: {len(records_to_insert)})."
            )
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Database error inserting klines: {e}")
            print(f"Problematic batch (first 5 if large): {records_to_insert[:5]}")
    return inserted_count


def fetch_and_store_historical_klines(
    b_client,
    db_conn,
    symbol_id,
    instrument_name,
    interval,
    start_str,
    end_str=None,
    kline_batch_size=500,
):
    print(
        f"Fetching Klines: {instrument_name} interval {interval} from {start_str} to {end_str or 'latest'}..."
    )
    total_processed = 0
    try:
        klines_generator = b_client.get_historical_klines_generator(
            instrument_name, interval, start_str, end_str=end_str
        )
        batch = []
        for kline in klines_generator:
            batch.append(kline)
            if len(batch) >= kline_batch_size:
                processed_in_batch = store_klines_batch(
                    db_conn, batch, symbol_id, interval, kline_batch_size
                )
                total_processed += processed_in_batch
                batch = []
                if processed_in_batch > 0:  # Only sleep if we actually did work
                    time.sleep(0.1)
        if batch:
            processed_in_batch = store_klines_batch(
                db_conn, batch, symbol_id, interval, kline_batch_size
            )
            total_processed += processed_in_batch
        print(
            f"Finished fetching klines for {instrument_name}. Total processed (inserted/updated): {total_processed}"
        )
    except Exception as e:
        print(f"An error occurred fetching/storing klines for {instrument_name}: {e}")
    return total_processed


# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and store Binance kline data into PostgreSQL/TimescaleDB."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to configuration file (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--symbol", type=str, default="BTCUSDT", help="Trading symbol (e.g., BTCUSDT)."
    )
    parser.add_argument(
        "--base_asset",
        type=str,
        default=None,
        help="Base asset (e.g., BTC). If blank, script will try to infer.",
    )
    parser.add_argument(
        "--quote_asset",
        type=str,
        default=None,
        help="Quote asset (e.g., USDT). If blank, script will try to infer.",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default=Client.KLINE_INTERVAL_1MINUTE,
        help="Kline interval (e.g., 1m). Primarily intended for 1m source data.",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default="1 day ago UTC",
        help="Kline start date (e.g., '1 Jan, 2020', '2020-01-01', '1 day ago UTC').",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="Kline end date. If not provided, fetches up to most recent.",
    )
    args = parser.parse_args()

    try:
        config_object = load_config(args.config)
    except FileNotFoundError as e:
        print(e)
        exit(1)
    except KeyError as e:
        print(f"Configuration error: {e}")
        exit(1)

    kline_db_batch_size = int(
        config_object.get("settings", "kline_fetch_batch_size", fallback=500)
    )

    db_connection = None
    try:
        db_connection = get_db_connection(config_object)
        binance_client = get_binance_client(config_object)
        print("Successfully connected to database and initialized Binance client.")

        with db_connection.cursor() as cursor:
            exchange_id = get_or_create_exchange_id(cursor, "Binance")
            symbol_id = get_or_create_symbol_id(
                cursor, exchange_id, args.symbol, args.base_asset, args.quote_asset
            )
            db_connection.commit()

        print(
            f"Using Exchange ID: {exchange_id}, Symbol ID: {symbol_id} for Instrument: {args.symbol}"
        )

        fetch_and_store_historical_klines(
            binance_client,
            db_connection,
            symbol_id,
            args.symbol,
            args.interval,
            args.start_date,
            args.end_date,
            kline_db_batch_size,
        )

        print("\nKline data fetching script finished.")

    except psycopg2.Error as db_err:
        print(f"A database error occurred: {db_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if db_connection:
            db_connection.close()
            print("Database connection closed.")
