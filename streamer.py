import logging
import os

from dotenv import load_dotenv
from snowflake.ingest.streaming import StreamingIngestClient

from utils import configure_logging

configure_logging()
logger = logging.getLogger("ski_data_streamer")

load_dotenv()

ACK_TIMEOUT_SECONDS = int(os.getenv("ACK_TIMEOUT_SECONDS", "120"))


def _required_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _account_uri_parts():
    """Parse the Snowsight Account URL. Do not synthesize hostnames from a locator."""
    uri = _required_env("SNOWFLAKE_ACCOUNT_URI").strip()
    if "://" not in uri:
        uri = "https://" + uri
    uri = uri.rstrip("/")
    host = uri.split("://", 1)[1].split("/", 1)[0]
    if ":" in host:
        host = host.split(":", 1)[0]
    account = host.removesuffix(".snowflakecomputing.com")
    if account.endswith(".privatelink"):
        account = account[: -len(".privatelink")]
    return uri, account


class SnowflakeStreamingSink:
    """Send generated rows directly through durable elastic channels."""

    def __init__(self):
        account_url, account = _account_uri_parts()
        properties = {
            "account": account,
            "user": _required_env("SNOWFLAKE_USER"),
            "private_key": _required_env("PRIVATE_KEY"),
            "url": account_url,
        }
        role = os.getenv("SNOWFLAKE_ROLE")
        if role:
            properties["role"] = role

        database = _required_env("DATABASE_NAME")
        schema = _required_env("SCHEMA_NAME")
        client_name = _required_env("CLIENT_NAME")
        pipe_names = {
            "resort_tickets": _required_env("RESORT_TICKET_PIPE_NAME"),
            "season_passes": _required_env("SEASON_PASS_PIPE_NAME"),
            "lift_rides": _required_env("LIFT_RIDE_PIPE_NAME"),
        }

        self._clients = {}
        self._channels = {}
        self._append_seq = 0
        try:
            for stream_name, pipe_name in pipe_names.items():
                client = StreamingIngestClient(
                    client_name=f"{client_name}-{stream_name}",
                    db_name=database,
                    schema_name=schema,
                    pipe_name=pipe_name,
                    properties=properties,
                )
                self._clients[stream_name] = client
                self._channels[stream_name] = client.get_elastic_channel()
                logger.info("Opened elastic channel for %s", pipe_name)
        except Exception:
            self.close()
            raise

    def append_batches(self, batches):
        """Append each non-empty batch and wait for Snowflake's durable ack."""
        pending = []
        for stream_name, rows in batches.items():
            if rows:
                self._append_seq += 1
                future = self._channels[stream_name].append_rows_with_wait(
                    rows, f"{stream_name}-{self._append_seq}"
                )
                pending.append((stream_name, len(rows), future))

        for stream_name, row_count, future in pending:
            future.result(timeout=ACK_TIMEOUT_SECONDS)
            logger.debug(
                "Snowflake durably acknowledged %d %s rows",
                row_count,
                stream_name,
            )

    def close(self):
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                logger.exception("Failed to close a Snowflake streaming client")
        self._clients.clear()
        self._channels.clear()
