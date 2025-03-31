#!/usr/bin/env python

import logging
import os
import sqlite3

from contextlib import closing
from dotenv import load_dotenv

from snowflake.ingest import SnowflakeStreamingIngestClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
load_dotenv()

# parameters
channel_name = os.getenv("CHANNEL_NAME")
database_name = os.getenv("DATABASE_NAME")
schema_name = os.getenv("SCHEMA_NAME")
pipe_name = os.getenv("PIPE_NAME")
client_name = os.getenv("CLIENT_NAME")

props = {"account": os.getenv("SNOWFLAKE_ACCOUNT"), 
         "user": os.getenv("SNOWFLAKE_USER"),
         "database": database_name,
         "schema": schema_name,
         "private_key": "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)",
         "ROWSET_DEV_VM_TEST_MODE": "false",
         }
    

def main():
    with closing(SnowflakeStreamingIngestClient(client_name, **props)) as client:
        channel = client.open_channel(channel_name, database_name, schema_name, pipe_name)

        # get the current committed offset token
        latest_committed_offset_token = channel.get_latest_committed_offset_token()
        logger.info("latest committed offset token before insert: " + (latest_committed_offset_token or "None"))
        if not latest_committed_offset_token:
            latest_committed_offset_token = 0
        # send data with batching
        batch_size = 100
        logger.info("start sending insert rows with batching")
        con = sqlite3.connect("data.db")
        while True:
            cur = con.cursor()
            rows = []
            last_offset_token = None
            for row in cur.execute("SELECT * FROM tdata WHERE id > ? ORDER BY id ASC LIMIT ?", (int(latest_committed_offset_token), batch_size)):
                rows.append(row[1])
                last_offset_token = row[0]
            if len(rows) > 0 and last_offset_token:
                nl_json = "\n".join(rows)
                channel.insert_rows(nl_json, offset_token=str(last_offset_token))
                logger.info("inserted rows in batch to offset token: " + str(last_offset_token))
                # cleanup from committed offset
                current_committed_offset_token = channel.get_latest_committed_offset_token()
                if current_committed_offset_token and int(current_committed_offset_token) > int(latest_committed_offset_token):
                    logger.info("delete from latest committed offset token: " + current_committed_offset_token)
                    cur.execute("DELETE FROM tdata WHERE id <= ?", (int(current_committed_offset_token),))
                    con.commit()
                    latest_committed_offset_token = current_committed_offset_token
     
    
if __name__ == "__main__":
    main()