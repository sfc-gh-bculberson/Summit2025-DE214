#!/usr/bin/env python

import logging
import os
import threading
import uuid

from contextlib import closing
from dotenv import load_dotenv
from SQLiteBackend import SQLiteBackend

from snowflake.ingest import SnowflakeStreamingIngestClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
load_dotenv()

# parameters
# TODO: Reuse Channels
# It is too difficult to debug now when recreating the PIPE due to the following issue
# https://snowflakecomputing.atlassian.net/browse/SNOW-2017222
channel_name = str(uuid.uuid4())
database_name = os.getenv("DATABASE_NAME")
schema_name = os.getenv("SCHEMA_NAME")
client_name = os.getenv("CLIENT_NAME")
BATCH_SIZE = 2

props = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "database": database_name,
    "schema": schema_name,
    "private_key": "-----BEGIN PRIVATE KEY-----\n"
    + os.getenv("PRIVATE_KEY")
    + "\n-----END PRIVATE KEY-----\n)",
    "ROWSET_DEV_VM_TEST_MODE": "false",
}


def stream_resort_tickets():
    pipe_name = os.getenv("RESORT_TICKET_PIPE_NAME")
    backend = SQLiteBackend()
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        logger.info("start sending insert rows with batching for resort tickets")
        channel = client.open_channel(
            channel_name, database_name, schema_name, pipe_name
        )
        latest_committed_offset_token = channel.get_latest_committed_offset_token()
        if not latest_committed_offset_token:
            latest_committed_offset_token = 0
        while True:
            rows = backend.GetResortTicketBatch(
                latest_committed_offset_token, BATCH_SIZE
            )
            if len(rows) > 0:
                nl_json = "\n".join([row[1] for row in rows])
                latest_committed_offset_token = rows[-1][0]
                channel.insert_rows(nl_json, offset_token=latest_committed_offset_token)
                current_committed_offset_token = (
                    channel.get_latest_committed_offset_token()
                )
                if current_committed_offset_token:
                    backend.DeleteResortTickets(current_committed_offset_token)


def stream_season_passes():
    pipe_name = os.getenv("SEASON_PASS_PIPE_NAME")
    backend = SQLiteBackend()
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        logger.info("start sending insert rows with batching for season passes")
        channel = client.open_channel(
            channel_name, database_name, schema_name, pipe_name
        )
        latest_committed_offset_token = channel.get_latest_committed_offset_token()
        if not latest_committed_offset_token:
            latest_committed_offset_token = 0
        while True:
            rows = backend.GetSeasonPassBatch(latest_committed_offset_token, BATCH_SIZE)
            if len(rows) > 0:
                nl_json = "\n".join([row[1] for row in rows])
                latest_committed_offset_token = rows[-1][0]
                channel.insert_rows(nl_json, offset_token=latest_committed_offset_token)
                current_committed_offset_token = (
                    channel.get_latest_committed_offset_token()
                )
                if current_committed_offset_token:
                    backend.DeleteSeasonPasses(current_committed_offset_token)


def stream_lift_rides():
    pipe_name = os.getenv("LIFT_RIDE_PIPE_NAME")
    backend = SQLiteBackend()
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        logger.info("start sending insert rows with batching for lift rides")
        channel = client.open_channel(
            channel_name, database_name, schema_name, pipe_name
        )
        latest_committed_offset_token = channel.get_latest_committed_offset_token()
        if not latest_committed_offset_token:
            latest_committed_offset_token = 0
        while True:
            rows = backend.GetLiftRideBatch(latest_committed_offset_token, BATCH_SIZE)
            if len(rows) > 0:
                nl_json = "\n".join([row[1] for row in rows])
                latest_committed_offset_token = rows[-1][0]
                channel.insert_rows(nl_json, offset_token=latest_committed_offset_token)
                current_committed_offset_token = (
                    channel.get_latest_committed_offset_token()
                )
                if current_committed_offset_token:
                    backend.DeleteLiftRides(current_committed_offset_token)


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
