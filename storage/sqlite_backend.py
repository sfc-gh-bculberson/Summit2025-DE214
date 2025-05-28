import sqlite3
import time
import random
import logging
from threading import Lock

from utils import configure_logging

# Configure logger
configure_logging()
logger = logging.getLogger('sqlite_backend')

class SQLiteBackend:
    """SQLite backend for persistent storage of streaming data to ensure durability"""

    def __init__(self, db_path="/app/data/data.db", timeout=30.0, max_retries=5):
        """Initialize the SQLite backend

        Args:
            db_path: Path to the SQLite database file
            timeout: SQLite connection timeout in seconds
            max_retries: Maximum number of retries for locked database errors
        """
        self.db_path = db_path
        self.timeout = timeout
        self.max_retries = max_retries
        self.lock = Lock()

        # Initialize connection
        self.con = sqlite3.connect(self.db_path, timeout=self.timeout)

        # Enable WAL mode for better concurrency
        self.con.execute('PRAGMA journal_mode=WAL')

        # Create tables if they don't exist
        self._initialize_tables()

    def _initialize_tables(self):
        """Initialize database tables"""
        tables = [
            "CREATE TABLE IF NOT EXISTS SEASON_PASS (ID INTEGER PRIMARY KEY AUTOINCREMENT, DATA TEXT)",
            "CREATE TABLE IF NOT EXISTS RESORT_TICKET (ID INTEGER PRIMARY KEY AUTOINCREMENT, DATA TEXT)",
            "CREATE TABLE IF NOT EXISTS LIFT_RIDE (ID INTEGER PRIMARY KEY AUTOINCREMENT, DATA TEXT)"
        ]

        with self.lock:
            cur = self.con.cursor()
            for table_sql in tables:
                cur.execute(table_sql)
                self.con.commit()

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Execute a database operation with retries for database locks

        Args:
            operation: Function to execute within retry logic
            *args: Arguments to pass to the operation function
            **kwargs: Keyword arguments to pass to the operation function

        Returns:
            The result of the operation function

        Raises:
            sqlite3.OperationalError: If the database remains locked after max retries
            Exception: Any other exceptions from the operation function
        """
        retries = 0
        delay = 0.1  # Initial delay in seconds

        while True:
            try:
                return operation(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and retries < self.max_retries:
                    retries += 1
                    # Log retry attempt
                    logger.warning(f"Database locked, retrying ({retries}/{self.max_retries})...")

                    # Sleep with exponential backoff and some randomness
                    time.sleep(delay * (0.8 + 0.4 * random.random()))
                    delay = min(delay * 2, 5.0)  # Cap at 5 seconds
                else:
                    # We've exhausted retries or it's a different error
                    if "database is locked" in str(e):
                        logger.error(f"Database still locked after {self.max_retries} retries")
                    raise

    # Store operations with retry logic

    def StoreSeasonPass(self, season_pass):
        """Store a season pass with retry logic"""
        data = season_pass.to_json()

        def _insert():
            with self.lock:
                cur = self.con.cursor()
                cur.execute("INSERT INTO SEASON_PASS (DATA) VALUES (?)", (data,))
                self.con.commit()

        self._execute_with_retry(_insert)

    def StoreResortTicket(self, resort_ticket):
        """Store a resort ticket with retry logic"""
        data = resort_ticket.to_json()

        def _insert():
            with self.lock:
                cur = self.con.cursor()
                cur.execute("INSERT INTO RESORT_TICKET (DATA) VALUES (?)", (data,))
                self.con.commit()

        self._execute_with_retry(_insert)

    def StoreLiftRide(self, lift_ride):
        """Store a lift ride with retry logic"""
        data = lift_ride.to_json()

        def _insert():
            with self.lock:
                cur = self.con.cursor()
                cur.execute("INSERT INTO LIFT_RIDE (DATA) VALUES (?)", (data,))
                self.con.commit()

        self._execute_with_retry(_insert)

    # Batch retrieve operations with retry logic

    def GetSeasonPassBatch(self, after_id, batch_size):
        """Get a batch of season passes with retry logic"""
        def _select():
            with self.lock:
                cur = self.con.cursor()
                cur.execute(
                    "SELECT ID, DATA FROM SEASON_PASS WHERE ID > ? LIMIT ?",
                    (after_id, batch_size)
                )
                return cur.fetchall()

        return self._execute_with_retry(_select)

    def GetResortTicketBatch(self, after_id, batch_size):
        """Get a batch of resort tickets with retry logic"""
        def _select():
            with self.lock:
                cur = self.con.cursor()
                cur.execute(
                    "SELECT ID, DATA FROM RESORT_TICKET WHERE ID > ? LIMIT ?",
                    (after_id, batch_size)
                )
                return cur.fetchall()

        return self._execute_with_retry(_select)

    def GetLiftRideBatch(self, after_id, batch_size):
        """Get a batch of lift rides with retry logic"""
        def _select():
            with self.lock:
                cur = self.con.cursor()
                cur.execute(
                    "SELECT ID, DATA FROM LIFT_RIDE WHERE ID > ? LIMIT ?",
                    (after_id, batch_size)
                )
                return cur.fetchall()

        return self._execute_with_retry(_select)

    # Delete operations with retry logic

    def DeleteSeasonPasses(self, before_id):
        """Delete season passes with retry logic"""
        def _delete():
            with self.lock:
                cur = self.con.cursor()
                cur.execute("DELETE FROM SEASON_PASS WHERE ID <= ?", (before_id,))
                self.con.commit()

        self._execute_with_retry(_delete)

    def DeleteResortTickets(self, before_id):
        """Delete resort tickets with retry logic"""
        def _delete():
            with self.lock:
                cur = self.con.cursor()
                cur.execute("DELETE FROM RESORT_TICKET WHERE ID <= ?", (before_id,))
                self.con.commit()

        self._execute_with_retry(_delete)

    def DeleteLiftRides(self, before_id):
        """Delete lift rides with retry logic"""
        def _delete():
            with self.lock:
                cur = self.con.cursor()
                cur.execute("DELETE FROM LIFT_RIDE WHERE ID <= ?", (before_id,))
                self.con.commit()

        self._execute_with_retry(_delete)