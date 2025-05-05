#!/usr/bin/env python

import logging
import os
import threading

from contextlib import closing
from dotenv import load_dotenv
from SQLiteBackend import SQLiteBackend

from snowflake.ingest import SnowflakeStreamingIngestClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
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


def stream_data(pipe_name, fn_get_data, fn_delete_data):
    # Write this function to stream data to Snowflake
    pass


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
    fns = [
        threading.Thread(target=stream_resort_tickets),
        threading.Thread(target=stream_season_passes),
        threading.Thread(target=stream_lift_rides),
    ]
    for fn in fns:
        fn.start()
    for fn in fns:
        fn.join()


if __name__ == "__main__":
    main()
