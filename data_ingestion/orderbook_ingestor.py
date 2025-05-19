import time
import json
import signal
import psycopg2
import argparse
from datetime import datetime

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


# --- Global flag for shutdown ---
shutdown_flag = False


def signal_handler(signum, frame):
    global shutdown_flag
    print(
        f"Signal {signum} received, initiating graceful shutdown for order book collector..."
    )
    shutdown_flag = True


# Register signal handlers for SIGINT (Ctrl+C) and SIGTERM
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def store_order_book_snapshot(db_conn, symbol_id, instrument_name, depth_data):
    snapshot_time = datetime.utcnow()
    bids_json = json.dumps(
        depth_data["bids"]
    )  # Bids and asks from Binance are already lists of [price_str, quantity_str]
    asks_json = json.dumps(depth_data["asks"])
    last_update_id = depth_data["lastUpdateId"]

    try:
        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO order_book_snapshots (time, symbol_id, last_update_id, bids, asks, retrieved_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, symbol_id, last_update_id) DO NOTHING;
                """,
                (
                    snapshot_time,
                    symbol_id,
                    last_update_id,
                    bids_json,
                    asks_json,
                    snapshot_time,
                ),
            )
            db_conn.commit()
            if cursor.rowcount > 0:
                print(
                    f"[{snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}] Stored {instrument_name} order book (LUID: {last_update_id})."
                )
                return True
            else:
                # This might happen if script restarts and tries to insert for same second with same LUID
                print(
                    f"[{snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}] {instrument_name} order book snapshot (LUID: {last_update_id}) likely already exists or no change."
                )
                return False
    except psycopg2.Error as e:
        db_conn.rollback()
        print(f"Database error storing order book for {instrument_name}: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error storing order book for {instrument_name}: {e}")
        return False


def main_polling_loop(
    b_client, db_conn, symbol_id, instrument_name, depth_limit, interval_seconds
):
    global shutdown_flag
    print(
        f"Starting order book snapshot collection for {instrument_name} every {interval_seconds} seconds. Press Ctrl+C to stop."
    )

    successful_stores = 0
    failed_fetches = 0

    while not shutdown_flag:
        loop_start_time = time.time()
        try:
            current_depth = b_client.get_order_book(
                symbol=instrument_name, limit=depth_limit
            )
            if current_depth and "bids" in current_depth and "asks" in current_depth:
                if store_order_book_snapshot(
                    db_conn, symbol_id, instrument_name, current_depth
                ):
                    successful_stores += 1
            else:
                failed_fetches += 1
                print(
                    f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Failed to fetch valid order book for {instrument_name}. Total fails: {failed_fetches}"
                )

        except Exception as e:
            failed_fetches += 1
            print(
                f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Error during fetch/store for {instrument_name}: {e}. Total fails: {failed_fetches}"
            )
            # TODO: Add more robust error handling for API errors
            if "rate limit" in str(e).lower() or "1003" in str(e):
                print("Rate limit likely hit, sleeping for 60 seconds...")
                time.sleep(60)

        # Calculate time to sleep to maintain the desired interval
        elapsed_time = time.time() - loop_start_time
        sleep_duration = interval_seconds - elapsed_time

        if shutdown_flag:  # Check flag again before sleeping
            break

        if sleep_duration > 0:
            # Sleep in smaller chunks to check shutdown_flag more frequently
            for _ in range(int(sleep_duration)):
                if shutdown_flag:
                    break
                time.sleep(1)
            if not shutdown_flag and sleep_duration % 1 > 0:
                time.sleep(sleep_duration % 1)

        if successful_stores > 0 and successful_stores % 100 == 0:  # Log progress
            print(
                f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Successfully stored {successful_stores} snapshots for {instrument_name}."
            )

    print(
        f"Order book collection loop stopped for {instrument_name}. Total stored: {successful_stores}, Fetch/Store errors: {failed_fetches}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Periodically collect Binance order book snapshots."
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
        "--limit",
        type=int,
        default=100,
        choices=[5, 10, 20, 50, 100, 500, 1000, 5000],
        help="Number of depth levels (default 100).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Collection interval in seconds (e.g., 60 for 1-minute snapshots).",
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

    db_connection = None
    try:
        db_connection = get_db_connection(config_object)
        binance_client = get_binance_client(config_object)
        print(
            "Successfully connected to database and initialized Binance client for order book collection."
        )

        with db_connection.cursor() as cursor:
            # Assuming exchange_id = 1 for "Binance" is already created by kline_ingestor
            # or can be created here too if get_or_create_exchange_id is in utils
            exchange_id_val = get_or_create_exchange_id(cursor, "Binance")
            # This script should not create symbols if they don't exist,
            # kline_ingestor or a dedicated symbol management should handle that.
            # We use a modified get_or_create_symbol_id that only gets or fails.
            # For simplicity, we'll reuse the one from utils that can create.
            symbol_id_val = get_or_create_symbol_id(
                cursor, exchange_id_val, args.symbol, base_asset=None, quote_asset=None
            )
            db_connection.commit()  # Commit if exchange/symbol was created

        if (
            symbol_id_val is None
        ):  # Should not happen if get_or_create_symbol_id is used
            print(f"Exiting: Symbol {args.symbol} could not be processed.")
            exit(1)

        print(
            f"Targeting Symbol ID: {symbol_id_val} for Instrument: {args.symbol} for order book snapshots."
        )

        main_polling_loop(
            binance_client,
            db_connection,
            symbol_id_val,
            args.symbol,
            args.limit,
            args.interval,
        )

    except psycopg2.Error as db_err:
        print(f"A database error occurred: {db_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if db_connection:
            db_connection.close()
            print("Database connection closed for order book collector.")
        print("Snapshot collector shut down.")
