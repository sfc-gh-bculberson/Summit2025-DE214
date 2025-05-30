import logging


def configure_logging(level=logging.INFO):
    # Configure logging at startup
    logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # List of namespaces for which you want debug level logging
    debug_namespaces = ['snowflake.ingest.streaming']

    # Setting info level for specific namespaces
    for namespace in debug_namespaces:
        logger = logging.getLogger(namespace)
        logger.setLevel(logging.DEBUG)
