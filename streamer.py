#!/usr/bin/env python

import logging
import os
import threading

from contextlib import closing
from dotenv import load_dotenv

from snowflake.ingest import SnowflakeStreamingIngestClient

from storage.sqlite_backend import SQLiteBackend
from utils import configure_logging

configure_logging(logging.DEBUG)
logger = logging.getLogger('ski_data_streamer')

load_dotenv()

# parameters
channel_name = "DE214-CODESPACE-2"
database_name = os.getenv("DATABASE_NAME")
schema_name = os.getenv("SCHEMA_NAME")
client_name = os.getenv("CLIENT_NAME")
account_name = os.getenv("SNOWFLAKE_ACCOUNT")
user_name = os.getenv("SNOWFLAKE_USER")
private_key = os.getenv("PRIVATE_KEY")
BATCH_SIZE = 10000


def stream_data(pipe_name, fn_get_data, fn_delete_data):
    props = {
        "account": account_name,
        "user": user_name,
        "database": database_name,
        "schema": schema_name,
        "private_key": private_key,
        "ROWSET_DEV_VM_TEST_MODE": "false",
    }
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        logger.info("sending rows with batching")

        channel = client.open_channel(
            channel_name, database_name, schema_name, pipe_name
        )
        latest_committed_offset_token = channel.get_latest_committed_offset_token()
        if not latest_committed_offset_token:
            latest_committed_offset_token = 0
        while True:
            rows = fn_get_data(latest_committed_offset_token, BATCH_SIZE)
            if len(rows) > 0:
                nl_json = "\n".join([row[1] for row in rows])
                latest_committed_offset_token = rows[-1][0]
                channel.insert_rows(nl_json, offset_token=latest_committed_offset_token)
                current_committed_offset_token = (
                    channel.get_latest_committed_offset_token()
                )
                if current_committed_offset_token:
                    fn_delete_data(current_committed_offset_token)


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
