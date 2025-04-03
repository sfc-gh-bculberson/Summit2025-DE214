#!/usr/bin/env python

import logging
import os
import sqlite3
import threading
import uuid

from contextlib import closing
from dotenv import load_dotenv

from snowflake.ingest import SnowflakeStreamingIngestClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
load_dotenv()

# parameters
channel_name = str(uuid.uuid4())
database_name = os.getenv("DATABASE_NAME")
schema_name = os.getenv("SCHEMA_NAME")
client_name = os.getenv("CLIENT_NAME")
BATCH_SIZE = 1000

props = {"account": os.getenv("SNOWFLAKE_ACCOUNT"), 
         "user": os.getenv("SNOWFLAKE_USER"),
         "database": database_name,
         "schema": schema_name,
         "private_key": "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)",
         "ROWSET_DEV_VM_TEST_MODE": "false",
         }


def query_and_insert(channel, con, table):
    # get the current committed offset token
    latest_committed_offset_token = channel.get_latest_committed_offset_token()
    logger.info("latest committed offset token in " + table + " before insert: " + (latest_committed_offset_token or "None"))
    if not latest_committed_offset_token:
        latest_committed_offset_token = 0
    while True:
        cur = con.cursor()
        rows = []
        last_offset_token = None
        for row in cur.execute("SELECT * FROM " + table + " WHERE id > ? ORDER BY id ASC LIMIT ?", (int(latest_committed_offset_token), BATCH_SIZE)):
            rows.append(row[1])
            last_offset_token = row[0]
        if len(rows) > 0 and last_offset_token:
            nl_json = "\n".join(rows)
            channel.insert_rows(nl_json, offset_token=str(last_offset_token))
            logger.info("inserted rows in batch to offset token: " + str(last_offset_token))
            # cleanup from committed offset
            current_committed_offset_token = channel.get_latest_committed_offset_token()
            if current_committed_offset_token and int(current_committed_offset_token) > int(latest_committed_offset_token):
                logger.info("delete from latest committed offset token in " + table + ": " + current_committed_offset_token)
                cur.execute("DELETE FROM " + table + " WHERE id <= ?", (int(current_committed_offset_token),))
                con.commit()
                latest_committed_offset_token = current_committed_offset_token


def stream_resort_tickets():
    pipe_name = os.getenv("RESORT_TICKET_PIPE_NAME")
    con = sqlite3.connect("data.db")
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        logger.info("start sending insert rows with batching for resort tickets")
        channel = client.open_channel(channel_name, database_name, schema_name, pipe_name)
        query_and_insert(channel, con, "resort_ticket")


def stream_season_passes():
    pipe_name = os.getenv("SEASON_PASS_PIPE_NAME")
    con = sqlite3.connect("data.db")
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        logger.info("start sending insert rows with batching for season passes")
        channel = client.open_channel(channel_name, database_name, schema_name, pipe_name)
        query_and_insert(channel, con, "season_pass")
        

def stream_lift_rides():
    pipe_name = os.getenv("LIFT_RIDE_PIPE_NAME")
    con = sqlite3.connect("data.db")
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        logger.info("start sending insert rows with batching for lift rides")
        channel = client.open_channel(channel_name, database_name, schema_name, pipe_name)
        query_and_insert(channel, con, "lift_ride")

        
def main():
        fns = [threading.Thread(target=stream_resort_tickets), threading.Thread(target=stream_season_passes), threading.Thread(target=stream_lift_rides)]
        for fn in fns:
            fn.start()            
        for fn in fns:
            fn.join()
    

if __name__ == "__main__":
    main()