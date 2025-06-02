import logging
import os
import threading
import time

from contextlib import closing
from pathlib import Path
from dotenv import load_dotenv

from snowflake.ingest import SnowflakeStreamingIngestClient

from storage.sqlite_backend import SQLiteBackend
from utils import configure_logging

configure_logging(logging.DEBUG)
logger = logging.getLogger('ski_data_streamer')

load_dotenv()

# parameters
channel_name = "DE214-CODESPACE"
database_name = os.getenv("DATABASE_NAME")
schema_name = os.getenv("SCHEMA_NAME")
client_name = os.getenv("CLIENT_NAME")
account_name = os.getenv("SNOWFLAKE_ACCOUNT")
user_name = os.getenv("SNOWFLAKE_USER")
private_key = os.getenv("PRIVATE_KEY")
BATCH_SIZE = 10000

LOOP_LOG_INTERVAL_SECONDS = 10  # Log every N seconds


def stream_data(pipe_name, fn_get_data, fn_delete_data):
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Starting stream for pipe: {pipe_name}")

    props = {
        "account": account_name,
        "user": user_name,
        "database": database_name,
        "schema": schema_name,
        "private_key": private_key,
        "ROWSET_DEV_VM_TEST_MODE": "false",
    }
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        logger.info(f"[{thread_name}] Connected to Snowflake, sending rows with batching")

        channel = client.open_channel(
            channel_name, database_name, schema_name, pipe_name
        )
        logger.info(f"[{thread_name}] Opened channel for {pipe_name}")

        latest_committed_offset_token = channel.get_latest_committed_offset_token()
        if not latest_committed_offset_token:
            latest_committed_offset_token = 0

        loop_count = 0
        last_log_time = time.time()

        while True:
            loop_count += 1
            rows = fn_get_data(latest_committed_offset_token, BATCH_SIZE)

            # Periodic logging to show we're still running
            current_time = time.time()
            if current_time - last_log_time >= LOOP_LOG_INTERVAL_SECONDS:
                logger.info(f"[{thread_name}] Loop #{loop_count} - Fetched {len(rows)} rows from offset {latest_committed_offset_token}")
                last_log_time = current_time

            if len(rows) > 0:
                nl_json = "\n".join([row[1] for row in rows])
                latest_committed_offset_token = rows[-1][0]
                channel.insert_rows(nl_json, offset_token=latest_committed_offset_token)
                current_committed_offset_token = (
                    channel.get_latest_committed_offset_token()
                )
                if current_committed_offset_token:
                    fn_delete_data(current_committed_offset_token)
                    logger.debug(f"[{thread_name}] Processed {len(rows)} rows, deleted up to offset {current_committed_offset_token}")

def stream_resort_tickets():
    pipe_name = os.getenv("RESORT_TICKET_PIPE_NAME")
    backend = SQLiteBackend()
    stream_data(pipe_name, backend.GetResortTicketBatch, backend.DeleteResortTickets)


def stream_season_passes():
    pipe_name = os.getenv("SEASON_PASS_PIPE_NAME")
    backend = SQLiteBackend()
    stream_data(pipe_name, backend.GetSeasonPassBatch, backend.DeleteSeasonPasses)


def stream_lift_rides():
    pipe_name = os.getenv("LIFT_RIDE_PIPE_NAME")
    backend = SQLiteBackend()
    stream_data(pipe_name, backend.GetLiftRideBatch, backend.DeleteLiftRides)


def main():
    logger.info("Starting ski data streamer with 3 threads")

    fns = [
        threading.Thread(target=stream_resort_tickets, name="ResortTickets"),
        threading.Thread(target=stream_season_passes, name="SeasonPasses"),
        threading.Thread(target=stream_lift_rides, name="LiftRides"),
    ]

    logger.info(f"Created {len(fns)} threads")

    for fn in fns:
        fn.start()
        logger.info(f"Started thread: {fn.name}")

    logger.info("All threads started, waiting for completion...")

    for fn in fns:
        fn.join()


if __name__ == "__main__":
    main()