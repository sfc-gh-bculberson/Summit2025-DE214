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
host_name = os.getenv("SNOWFLAKE_HOST", f"{account_name}.snowflakecomputing.com").replace("_","-")
user_name = os.getenv("SNOWFLAKE_USER")
private_key = os.getenv("PRIVATE_KEY")
BATCH_SIZE = 10000

LOOP_LOG_INTERVAL_SECONDS = 10  # Log every N seconds


def stream_data(pipe_name, fn_get_data, fn_delete_data):
    # Write this function to stream data to Snowflake
    while True:
        logger.info("Sending rows with batching")


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