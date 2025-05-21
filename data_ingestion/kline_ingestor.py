import psycopg2
import argparse
import pandas as pd
from psycopg2.extras import execute_values, DictCursor
from datetime import datetime as dt, timezone, timedelta  # dt alias used
from typing import Optional, List, Tuple, Any
from decimal import Decimal, InvalidOperation
import asyncio
import logging

from .utils import (
    load_config,
    get_db_connection,
    get_exchange_adapter,
    get_or_create_exchange_id,
    get_or_create_symbol_id,
    DEFAULT_CONFIG_PATH,
    setup_logging,
)
from .exchanges.base_interface import ExchangeInterface
from .exchanges.symbol_representation import SymbolRepresentation, FUTURE, PERP, SPOT


logger = logging.getLogger(__name__)


def store_klines_batch(
    conn,
    klines_data_df: pd.DataFrame,
    symbol_id: int,
    interval: str,
    exchange_name: str,
) -> int:
    if klines_data_df.empty:
        return 0

    records_to_insert: List[Tuple[Any, ...]] = []
    for _, kline_row in klines_data_df.iterrows():  # kline_row is a pandas Series
        try:
            open_time_utc = kline_row["time"]
            open_price_raw = kline_row["open"]
            high_price_raw = kline_row["high"]
            low_price_raw = kline_row["low"]
            close_price_raw = kline_row["close"]
            volume_raw = kline_row["volume"]

            quote_asset_volume_val_raw = kline_row.get("quote_volume")
            if (
                pd.isna(quote_asset_volume_val_raw)
                or quote_asset_volume_val_raw is None
            ):
                quote_asset_volume_val_decimal = Decimal("0.0")
            elif isinstance(quote_asset_volume_val_raw, Decimal):
                quote_asset_volume_val_decimal = quote_asset_volume_val_raw
            else:
                try:
                    quote_asset_volume_val_decimal = Decimal(
                        str(quote_asset_volume_val_raw)
                    )
                except InvalidOperation:
                    logger.warning(
                        f"Invalid operation converting quote_volume '{quote_asset_volume_val_raw}' to Decimal "
                        f"for symbol_id {symbol_id} at kline time {open_time_utc}. Defaulting to 0.0."
                    )
                    quote_asset_volume_val_decimal = Decimal("0.0")

            close_timestamp_val_raw = kline_row.get("close_timestamp")
            if pd.isna(close_timestamp_val_raw) or close_timestamp_val_raw is None:
                close_timestamp_val_dt = None
            elif isinstance(close_timestamp_val_raw, pd.Timestamp):
                close_timestamp_val_dt = close_timestamp_val_raw.to_pydatetime()
            else:
                logger.warning(
                    f"close_timestamp for symbol_id {symbol_id} at kline time {open_time_utc} is not a pd.Timestamp: {type(close_timestamp_val_raw)}. Attempting conversion."
                )
                try:
                    close_timestamp_val_dt = pd.to_datetime(
                        close_timestamp_val_raw, utc=True
                    ).to_pydatetime()
                except Exception as e_conv:
                    logger.error(
                        f"Failed to convert close_timestamp '{close_timestamp_val_raw}' to datetime (Error: {e_conv}). Setting to None."
                    )
                    close_timestamp_val_dt = None

            trade_count_val_raw = kline_row.get("trade_count")
            if pd.isna(trade_count_val_raw) or trade_count_val_raw is None:
                trade_count_val_int = None
            else:
                try:
                    trade_count_val_int = int(trade_count_val_raw)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid trade_count '{trade_count_val_raw}' for symbol_id {symbol_id} at kline time {open_time_utc}. Setting to None."
                    )
                    trade_count_val_int = None

            taker_base_volume_val_raw = kline_row.get("taker_base_volume")
            if pd.isna(taker_base_volume_val_raw) or taker_base_volume_val_raw is None:
                taker_base_volume_val_decimal = None
            elif isinstance(taker_base_volume_val_raw, Decimal):
                taker_base_volume_val_decimal = taker_base_volume_val_raw
            else:
                try:
                    taker_base_volume_val_decimal = Decimal(
                        str(taker_base_volume_val_raw)
                    )
                except InvalidOperation:
                    logger.warning(
                        f"Invalid taker_base_volume '{taker_base_volume_val_raw}' for symbol_id {symbol_id} at kline time {open_time_utc}. Setting to None."
                    )
                    taker_base_volume_val_decimal = None

            taker_quote_volume_val_raw = kline_row.get("taker_quote_volume")
            if (
                pd.isna(taker_quote_volume_val_raw)
                or taker_quote_volume_val_raw is None
            ):
                taker_quote_volume_val_decimal = None
            elif isinstance(taker_quote_volume_val_raw, Decimal):
                taker_quote_volume_val_decimal = taker_quote_volume_val_raw
            else:
                try:
                    taker_quote_volume_val_decimal = Decimal(
                        str(taker_quote_volume_val_raw)
                    )
                except InvalidOperation:
                    logger.warning(
                        f"Invalid taker_quote_volume '{taker_quote_volume_val_raw}' for symbol_id {symbol_id} at kline time {open_time_utc}. Setting to None."
                    )
                    taker_quote_volume_val_decimal = None

            if (
                not isinstance(open_time_utc, pd.Timestamp)
                or open_time_utc.tzinfo is None
            ):
                raise ValueError(
                    f"Kline time for symbol_id {symbol_id} is not a timezone-aware Pandas Timestamp (UTC): {open_time_utc}"
                )

            def to_decimal_or_raise(val, field_name, current_open_time):
                if pd.isna(val) or val is None:
                    raise ValueError(
                        f"Essential field '{field_name}' is None/NA for symbol_id {symbol_id} at kline time {current_open_time}"
                    )
                if isinstance(val, Decimal):
                    return val
                try:
                    return Decimal(str(val))
                except InvalidOperation:
                    raise ValueError(
                        f"Invalid decimal value for '{field_name}': '{val}' for symbol_id {symbol_id} at kline time {current_open_time}"
                    )

            open_price_d = to_decimal_or_raise(
                open_price_raw, "open_price", open_time_utc
            )
            high_price_d = to_decimal_or_raise(
                high_price_raw, "high_price", open_time_utc
            )
            low_price_d = to_decimal_or_raise(low_price_raw, "low_price", open_time_utc)
            close_price_d = to_decimal_or_raise(
                close_price_raw, "close_price", open_time_utc
            )
            volume_d = to_decimal_or_raise(volume_raw, "volume", open_time_utc)

            records_to_insert.append(
                (
                    open_time_utc.to_pydatetime(),
                    symbol_id,
                    interval,
                    open_price_d,
                    high_price_d,
                    low_price_d,
                    close_price_d,
                    volume_d,
                    quote_asset_volume_val_decimal,
                    close_timestamp_val_dt,
                    trade_count_val_int,
                    taker_base_volume_val_decimal,
                    taker_quote_volume_val_decimal,
                )
            )
        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            problematic_data_str = str(kline_row.to_dict())
            logger.warning(
                f"Skipping kline processing for symbol_id {symbol_id} from {exchange_name} due to error: {e}. "
                f"Problematic kline data (approx): {problematic_data_str[:300]}...",
                exc_info=False,
            )
            continue

    if not records_to_insert:
        return 0

    query = """
        INSERT INTO klines (time, symbol_id, interval, open_price, high_price, low_price, close_price,
                            volume, quote_asset_volume,
                            close_time, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
        VALUES %s
        ON CONFLICT (time, symbol_id, interval) DO UPDATE SET
            open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            quote_asset_volume = EXCLUDED.quote_asset_volume,
            close_time = EXCLUDED.close_time,
            number_of_trades = EXCLUDED.number_of_trades,
            taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
            ingested_at = CURRENT_TIMESTAMP;
    """
    inserted_count_for_batch = 0
    with conn.cursor() as cursor:
        try:
            execute_values(
                cursor, query, records_to_insert, page_size=len(records_to_insert)
            )
            conn.commit()
            inserted_count_for_batch = cursor.rowcount
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(
                f"DB error inserting klines for symbol_id {symbol_id} from {exchange_name} (batch size {len(records_to_insert)}): {e}",
                exc_info=True,
            )
            return 0
    return (
        inserted_count_for_batch
        if inserted_count_for_batch >= 0
        else len(records_to_insert)
    )


def get_local_kline_daterange(
    db_conn, symbol_id: int, interval: str
) -> Tuple[Optional[dt], Optional[dt]]:
    min_time_local, max_time_local = None, None
    try:
        with db_conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT MIN(time AT TIME ZONE 'UTC') as min_db_time, MAX(time AT TIME ZONE 'UTC') as max_db_time
                FROM klines
                WHERE symbol_id = %s AND interval = %s
                """,
                (symbol_id, interval),
            )
            result = cursor.fetchone()
            if result and result["min_db_time"] is not None:
                min_db_time_val = result["min_db_time"]
                max_db_time_val = result["max_db_time"]
                min_time_local = (
                    min_db_time_val.astimezone(timezone.utc)
                    if min_db_time_val.tzinfo
                    else min_db_time_val.replace(tzinfo=timezone.utc)
                )
                max_time_local = (
                    max_db_time_val.astimezone(timezone.utc)
                    if max_db_time_val.tzinfo
                    else max_db_time_val.replace(tzinfo=timezone.utc)
                )
    except psycopg2.Error as e:
        logger.error(
            f"DB Error checking local kline range for symbol_id {symbol_id}, interval {interval}: {e}",
            exc_info=True,
        )
    return min_time_local, max_time_local


def get_interval_timedelta(interval_str: str) -> timedelta:
    if not interval_str or len(interval_str) < 1:  # Allow 'm', 'h', etc.
        raise ValueError(f"Invalid interval string: '{interval_str}'")

    unit_part = interval_str[-1].lower()
    num_part_str = interval_str[:-1]

    try:
        if not num_part_str:  # Handles cases like 'm', 'h' which means 1m, 1h
            num_part = 1
        else:
            num_part = int(num_part_str)
        if num_part <= 0:
            raise ValueError("Numeric part of interval must be positive.")
    except ValueError:
        raise ValueError(f"Invalid numeric part in interval string: '{interval_str}'")

    if unit_part == "m":
        return timedelta(minutes=num_part)
    elif unit_part == "h":
        return timedelta(hours=num_part)
    elif unit_part == "d":
        return timedelta(days=num_part)
    elif unit_part == "w":
        return timedelta(weeks=num_part)
    else:
        raise ValueError(
            f"Unsupported interval unit '{unit_part}' in interval string: '{interval_str}'"
        )


async def fetch_and_store_historical_klines(
    exchange_adapter: ExchangeInterface,
    db_conn,
    symbol_id: int,
    standard_symbol_str: str,
    exchange_instrument_name: str,
    interval_str: str,
    requested_start_str_dt: str,
    requested_end_str_dt: Optional[str] = None,
    kline_chunk_size_for_db: int = 500,
) -> int:
    total_processed_for_symbol_run = 0
    exchange_name = exchange_adapter.get_exchange_name()
    log_prefix = f"[{exchange_instrument_name}@{exchange_name} (StdSym:{standard_symbol_str}) Interval:{interval_str}]"

    try:
        req_start_dt_orig = pd.to_datetime(
            requested_start_str_dt, errors="raise", utc=True
        )
    except Exception as e:
        logger.error(
            f"{log_prefix} Invalid start_date format: '{requested_start_str_dt}'. Error: {e}"
        )
        return 0

    req_end_dt_orig: Optional[pd.Timestamp] = None
    if requested_end_str_dt:
        try:
            req_end_dt_orig = pd.to_datetime(
                requested_end_str_dt, errors="raise", utc=True
            )
            if req_end_dt_orig < req_start_dt_orig:
                logger.warning(
                    f"{log_prefix} Requested end_date '{requested_end_str_dt}' is before start_date '{requested_start_str_dt}'. Skipping."
                )
                return 0
        except Exception as e:
            logger.error(
                f"{log_prefix} Invalid end_date format: '{requested_end_str_dt}'. Error: {e}"
            )
            return 0

    logger.info(
        f"{log_prefix} Request: Fetch from '{req_start_dt_orig.strftime('%Y-%m-%d %H:%M:%S %Z')}' to '{req_end_dt_orig.strftime('%Y-%m-%d %H:%M:%S %Z') if req_end_dt_orig else 'latest'}'"
    )

    min_time_local, max_time_local = get_local_kline_daterange(
        db_conn, symbol_id, interval_str
    )
    try:
        interval_delta = get_interval_timedelta(interval_str)
    except ValueError as e:
        logger.error(f"{log_prefix} {e}. Skipping symbol.")
        return 0

    if min_time_local and max_time_local:
        logger.info(
            f"{log_prefix} Local data found: {min_time_local.strftime('%Y-%m-%d %H:%M:%S %Z')} to {max_time_local.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )
    else:
        logger.info(f"{log_prefix} No local data found for this symbol/interval.")

    fetch_segments_to_process: List[
        Tuple[pd.Timestamp, Optional[pd.Timestamp], str]
    ] = []

    # Determine segments for backfill
    if not min_time_local or req_start_dt_orig < min_time_local:
        effective_end_for_backfill = (
            min_time_local - interval_delta if min_time_local else None
        )
        if req_end_dt_orig and (
            effective_end_for_backfill is None
            or req_end_dt_orig < effective_end_for_backfill
        ):
            effective_end_for_backfill = req_end_dt_orig

        if (
            effective_end_for_backfill is None
            or effective_end_for_backfill >= req_start_dt_orig
        ):
            fetch_segments_to_process.append(
                (
                    req_start_dt_orig,
                    effective_end_for_backfill,
                    "backfill / initial full range",
                )
            )

    # Determine segments for forward-fill
    start_for_forward_fill = req_start_dt_orig
    if max_time_local:
        start_for_forward_fill = max(max_time_local + interval_delta, req_start_dt_orig)

    # If a backfill segment was created and ended, ensure forward-fill starts after it
    if fetch_segments_to_process and fetch_segments_to_process[-1][1] is not None:
        if start_for_forward_fill <= fetch_segments_to_process[-1][1]:  # type: ignore
            start_for_forward_fill = fetch_segments_to_process[-1][1] + interval_delta  # type: ignore

    now_utc_approx = pd.Timestamp.utcnow()
    if start_for_forward_fill <= (
        now_utc_approx + interval_delta
    ):  # Only if start is not too far in future
        if req_end_dt_orig is None or req_end_dt_orig >= start_for_forward_fill:
            # If req_end_dt_orig is None, it's "to latest". If defined, ensure range is valid.
            desc = (
                "forward-fill (to latest)"
                if req_end_dt_orig is None
                else "forward-fill (to fixed end)"
            )
            fetch_segments_to_process.append(
                (start_for_forward_fill, req_end_dt_orig, desc)
            )

    # Basic deduplication and chronological sort
    unique_fetch_segments = sorted(
        list(set(fetch_segments_to_process)), key=lambda x: x[0]
    )

    final_fetch_segments = []
    for start_dt, end_dt, desc in unique_fetch_segments:
        if end_dt and start_dt > end_dt:
            logger.debug(
                f"{log_prefix} Filtering out invalid segment: Start {start_dt} > End {end_dt} for '{desc}'"
            )
            continue
        if start_dt > (
            now_utc_approx + interval_delta * 2
        ):  # Allow some buffer for very recent klines
            logger.debug(
                f"{log_prefix} Filtering out future segment: Start {start_dt} for '{desc}'"
            )
            continue
        final_fetch_segments.append((start_dt, end_dt, desc))

    if not final_fetch_segments:
        logger.info(
            f"{log_prefix} All data for the requested range seems to be locally available or request is for future data."
        )
        return 0

    for seg_idx, (segment_start_dt, segment_end_dt, segment_desc) in enumerate(
        final_fetch_segments
    ):
        current_fetch_start_dt_for_segment = segment_start_dt
        segment_fully_fetched = False
        api_calls_for_segment = 0
        # Max calls to prevent infinite loop if API/logic error. Adjust if very long history expected in one go.
        max_api_calls_per_segment = (
            2000  # e.g. 2000 * 1000 * 1min ~ 1388 days or ~3.8 years of 1m data
        )

        log_seg_start = segment_start_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        log_seg_end = (
            segment_end_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
            if segment_end_dt
            else "latest"
        )
        logger.info(
            f"{log_prefix} Segment {seg_idx+1}/{len(final_fetch_segments)} ({segment_desc}): API From '{log_seg_start}' to '{log_seg_end}'"
        )

        while (
            not segment_fully_fetched
            and api_calls_for_segment < max_api_calls_per_segment
        ):
            api_calls_for_segment += 1

            # The end datetime for this specific API call within the segment
            # If segment_end_dt is defined, the adapter (if it's python-binance) will page up to it.
            # If segment_end_dt is None (fetching to latest), we make one call and then advance our start time.
            current_api_call_end_dt_param = segment_end_dt

            logger.debug(
                f"{log_prefix} API Call #{api_calls_for_segment} for segment: Start {current_fetch_start_dt_for_segment}, End {current_api_call_end_dt_param or 'latest'}"
            )

            try:
                klines_df_this_api_call = await exchange_adapter.fetch_klines(
                    standard_symbol_str=standard_symbol_str,
                    interval=interval_str,
                    start_datetime=current_fetch_start_dt_for_segment,
                    end_datetime=current_api_call_end_dt_param,
                    limit=None,  # Adapter uses its default batch size (e.g., 1000 for Binance)
                )

                if klines_df_this_api_call.empty:
                    logger.info(
                        f"{log_prefix} API Call #{api_calls_for_segment}: No more klines returned. Assuming segment fully fetched or no data in range."
                    )
                    segment_fully_fetched = True
                    break

                # Store fetched klines
                # ... (DB chunking logic for store_klines_batch)
                if len(klines_df_this_api_call) > kline_chunk_size_for_db * 1.2:
                    num_chunks = (
                        len(klines_df_this_api_call) + kline_chunk_size_for_db - 1
                    ) // kline_chunk_size_for_db
                    logger.info(
                        f"{log_prefix} API Call #{api_calls_for_segment} returned {len(klines_df_this_api_call)} klines. Chunking into {num_chunks} for DB insert."
                    )
                    for i in range(num_chunks):
                        chunk_df = klines_df_this_api_call.iloc[
                            i
                            * kline_chunk_size_for_db : (i + 1)
                            * kline_chunk_size_for_db
                        ]
                        if not chunk_df.empty:
                            processed_count_chunk = store_klines_batch(
                                db_conn,
                                chunk_df,
                                symbol_id,
                                interval_str,
                                exchange_name,
                            )
                            total_processed_for_symbol_run += processed_count_chunk
                            logger.debug(
                                f"{log_prefix} DB Chunk {i+1}/{num_chunks}: Processed/Updated: {processed_count_chunk}."
                            )
                            if processed_count_chunk > 0:
                                await asyncio.sleep(0.02)  # Small breath
                else:
                    processed_count = store_klines_batch(
                        db_conn,
                        klines_df_this_api_call,
                        symbol_id,
                        interval_str,
                        exchange_name,
                    )
                    total_processed_for_symbol_run += processed_count
                    logger.info(
                        f"{log_prefix} API Call #{api_calls_for_segment} returned {len(klines_df_this_api_call)} klines, DB Processed/Updated: {processed_count}."
                    )
                    if processed_count > 0:
                        await asyncio.sleep(0.02)

                last_kline_time_fetched = klines_df_this_api_call["time"].iloc[-1]

                if segment_end_dt:  # Fetching towards a defined segment end
                    if last_kline_time_fetched >= segment_end_dt:
                        logger.info(
                            f"{log_prefix} Reached or passed segment end date {segment_end_dt}. Segment complete."
                        )
                        segment_fully_fetched = True
                    else:
                        # python-binance's get_historical_klines with start and end should fetch all in between.
                        # This loop for defined end_dt is a failsafe or for adapters that don't auto-page fully.
                        # If python-binance pages correctly, this segment should be fetched in one "API Call" from this loop's perspective.
                        # If it does, then this `else` block for advancing `current_fetch_start_dt_for_segment` when `segment_end_dt` is set,
                        # might not be strictly necessary if the adapter handles the full range.
                        # However, keeping it provides robustness for other adapters or partial fetches.
                        logger.info(
                            f"{log_prefix} Fetched up to {last_kline_time_fetched}, segment end is {segment_end_dt}. Assuming adapter handled full range or this was one page."
                        )
                        segment_fully_fetched = True  # Assume python-binance fetched all it could for the range.
                        # If not, the next segment will pick it up if ranges are defined correctly.
                else:  # Fetching "to latest" (segment_end_dt is None)
                    # If adapter's fetch_klines with end_datetime=None returns less than its typical max batch,
                    # assume we've hit the most recent data. (e.g. Binance default limit is 1000)
                    adapter_max_batch_heuristic = (
                        990  # Slightly less than typical max (e.g. 1000)
                    )
                    if len(klines_df_this_api_call) < adapter_max_batch_heuristic:
                        logger.info(
                            f"{log_prefix} Fetched {len(klines_df_this_api_call)} klines (less than heuristic max {adapter_max_batch_heuristic}). Assuming caught up to latest for now."
                        )
                        segment_fully_fetched = True
                    else:
                        current_fetch_start_dt_for_segment = (
                            last_kline_time_fetched + interval_delta
                        )
                        # Safety: don't let next start time go too far into the future
                        if current_fetch_start_dt_for_segment > (
                            pd.Timestamp.utcnow() + interval_delta * 5
                        ):  # Allow 5 intervals buffer
                            logger.warning(
                                f"{log_prefix} Next fetch start {current_fetch_start_dt_for_segment} is too far in the future. Stopping segment."
                            )
                            segment_fully_fetched = True

                if segment_fully_fetched:
                    break

                await asyncio.sleep(
                    0.1
                )  # Small delay between paged API calls if looping within segment

            except Exception as e_apicall:
                logger.error(
                    f"{log_prefix} Error during API call #{api_calls_for_segment} or DB store for segment: {e_apicall}",
                    exc_info=True,
                )
                await asyncio.sleep(5)  # Longer delay on error
                break  # Exit while loop for this segment on error, move to next segment or finish.

        if api_calls_for_segment >= max_api_calls_per_segment:
            logger.warning(
                f"{log_prefix} Reached max API calls ({max_api_calls_per_segment}) for segment. Moving on or finishing."
            )

    if total_processed_for_symbol_run > 0:
        logger.info(
            f"{log_prefix} Run Completed. Total DB klines processed/updated for this run: {total_processed_for_symbol_run}."
        )
    else:
        logger.info(
            f"{log_prefix} Run Completed. No new klines inserted/updated for this run."
        )
    return total_processed_for_symbol_run


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Download klines using exchange adapters."
    )
    # ... (Argument parsing remains the same as your last correct version) ...
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config file (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--exchange", type=str, default="binance", help="Exchange name (e.g., binance)."
    )
    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Standard trading symbol (e.g., BTC-USDT or BTC-USD-PERP).",
    )
    parser.add_argument(
        "--base-asset",
        type=str,
        help="Standard base asset (e.g., BTC). Optional, derived from --symbol if not provided.",
    )
    parser.add_argument(
        "--quote-asset",
        type=str,
        help="Standard quote asset (e.g., USDT or USD). Optional, derived from --symbol if not provided.",
    )
    parser.add_argument(
        "--instrument-type",
        type=str,
        help=f"Instrument type ({SPOT}, {PERP}, {FUTURE}). Optional, derived from --symbol or defaults to SPOT.",
    )
    parser.add_argument(
        "--interval", type=str, default="1m", help="Kline interval (e.g., 1m, 5m, 1h)."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Kline start date/datetime (UTC, e.g., YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Kline end date/datetime (UTC). Default: latest.",
    )
    args = parser.parse_args()

    async def main_cli():
        config_object = None
        db_connection = None
        exchange_adapter_instance: Optional[ExchangeInterface] = None

        try:
            config_object = load_config(args.config)
        except FileNotFoundError as e:
            logger.error(f"Configuration File Error: {e}")
            exit(1)
        except (KeyError, configparser.NoSectionError, configparser.NoOptionError) as e:
            logger.error(f"Configuration Key/Section Error: {e}")
            exit(1)

        kline_db_chunk_size_cfg = int(
            config_object.get("settings", "kline_fetch_batch_size", fallback="500")
        )

        try:
            db_connection = get_db_connection(config_object)
            if db_connection is None:
                logger.error("Failed to connect to the database. Exiting.")
                exit(1)

            exchange_adapter_instance = get_exchange_adapter(
                args.exchange, config_object
            )
            if hasattr(exchange_adapter_instance, "_ensure_cache_populated"):
                await exchange_adapter_instance._ensure_cache_populated()
            logger.info(
                f"Successfully connected to DB and initialized {args.exchange.capitalize()} adapter."
            )

            try:
                s_repr = SymbolRepresentation.parse(args.symbol)
            except ValueError as e:
                logger.error(f"Invalid --symbol argument '{args.symbol}': {e}")
                exit(1)

            base_for_db = (
                args.base_asset.upper() if args.base_asset else s_repr.base_asset
            )
            quote_for_db = (
                args.quote_asset.upper() if args.quote_asset else s_repr.quote_asset
            )

            type_for_db = s_repr.instrument_type
            if s_repr.instrument_type == FUTURE and s_repr.expiry_date:
                type_for_db = f"{FUTURE}_{s_repr.expiry_date}"

            if args.instrument_type:
                arg_instrument_type_upper = args.instrument_type.upper()
                if (
                    arg_instrument_type_upper.startswith(f"{FUTURE}_")
                    and len(arg_instrument_type_upper.split("_")) == 2
                    and arg_instrument_type_upper.split("_")[1].isdigit()
                ):
                    type_for_db = arg_instrument_type_upper
                elif arg_instrument_type_upper == FUTURE and s_repr.expiry_date:
                    type_for_db = f"{FUTURE}_{s_repr.expiry_date}"
                elif arg_instrument_type_upper in [SPOT, PERP]:
                    type_for_db = arg_instrument_type_upper
                elif arg_instrument_type_upper == FUTURE and not s_repr.expiry_date:
                    logger.warning(
                        f"--instrument-type=FUTURE provided but --symbol '{args.symbol}' has no expiry date. Storing as generic FUTURE type."
                    )
                    type_for_db = FUTURE
                else:
                    logger.warning(
                        f"Using '{arg_instrument_type_upper}' from --instrument-type. Ensure this is a valid storable type if it differs from parsed symbol."
                    )
                    type_for_db = arg_instrument_type_upper

            if not base_for_db or not quote_for_db:
                logger.error(
                    f"Could not determine standard base and quote assets from --symbol '{args.symbol}' or other arguments."
                )
                exit(1)

            exchange_instrument_name_for_db = ""
            try:
                exchange_instrument_name_for_db = (
                    exchange_adapter_instance.normalize_standard_symbol_to_exchange(
                        args.symbol
                    )
                )
            except ValueError as e:
                logger.error(
                    f"Failed to normalize standard symbol '{args.symbol}' to exchange format: {e}. Ensure adapter cache is populated or symbol is valid for the exchange."
                )
                exit(1)

            with db_connection.cursor(cursor_factory=DictCursor) as cursor:
                exchange_id_val = get_or_create_exchange_id(cursor, args.exchange)
                db_connection.commit()

                symbol_id_val, created = get_or_create_symbol_id(
                    cursor,
                    exchange_id_val,
                    exchange_instrument_name_for_db,
                    base_for_db,
                    quote_for_db,
                    type_for_db,
                )
                db_connection.commit()

            if symbol_id_val is None:
                logger.error(
                    f"Could not get/create symbol_id for {args.symbol} (Std: {base_for_db}-{quote_for_db}-{type_for_db}, Exch: {exchange_instrument_name_for_db}). Exiting."
                )
                exit(1)

            logger.info(
                f"Targeting Standard Symbol: {args.symbol} (Exchange: {args.exchange}, Exchange Instrument: {exchange_instrument_name_for_db}, DB ID: {symbol_id_val})"
            )

            await fetch_and_store_historical_klines(
                exchange_adapter_instance,
                db_connection,
                symbol_id_val,
                args.symbol,
                exchange_instrument_name_for_db,
                args.interval,
                args.start_date,
                args.end_date,
                kline_db_chunk_size_cfg,
            )
        except psycopg2.Error as db_err:
            logger.error(f"Database error: {db_err}", exc_info=True)
        except ValueError as ve:
            logger.error(f"Value error: {ve}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error in kline ingestor CLI: {e}", exc_info=True)
        finally:
            if db_connection:
                db_connection.close()
                logger.info("Database connection closed.")
            if exchange_adapter_instance and hasattr(
                exchange_adapter_instance, "close_session"
            ):
                await exchange_adapter_instance.close_session()
            logger.info("Kline data fetching script finished.")

    asyncio.run(main_cli())
