#!/usr/bin/env python

import logging
import os
from contextlib import closing
from time import sleep
from dotenv import load_dotenv

from snowflake.ingest import SnowflakeStreamingIngestClient
from generator import get_lift_ticket

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
load_dotenv()

# parameters
channel_name = os.getenv("CHANNEL_NAME")
database_name = os.getenv("DATABASE_NAME")
schema_name = os.getenv("SCHEMA_NAME")
pipe_name = os.getenv("PIPE_NAME")
client_name = os.getenv("CLIENT_NAME")

props = {"ssl": "on",
         "scheme": "https",
         "port": 443, 
         "url": os.getenv("SNOWFLAKE_URL"), 
         "account": os.getenv("SNOWFLAKE_ACCOUNT"), 
         "user": os.getenv("SNOWFLAKE_USER"),
         "database": database_name,
         "schema": schema_name,
         "private_key": "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)",
         }


with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
    channel = client.open_channel(channel_name, database_name, schema_name, pipe_name)
    row_num = 1000

    # get the current committed offset token
    latest_committed_offset_token = channel.get_latest_committed_offset_token()
    logger.info("latest committed offset token before insert: " + (latest_committed_offset_token or "None"))
    # send data with batching of 20 rows per batch
    batch_size = 20
    rows = []
    logger.info("start sending insert rows with batching")

    for val in range(row_num):
        rows.append(get_lift_ticket())
        if val % batch_size == 0 and val > 0:
            # batch insert
            nl_json = "\n".join(rows)
            channel.insert_rows(nl_json, offset_token=str(val))
            rows = []

    logger.info("insert rows with batching finishes")

    # sleep 10s to get everything committed
    sleep(10)
    latest_committed_offset_token = channel.get_latest_committed_offset_token()
    logger.info("latest committed offset token: " + (latest_committed_offset_token or "None"))

    channel.close()