import os
import datetime
import random
import logging
import time
import psutil

from utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger('ski_data_generator')

# Import models
from models.resort_ticket import ResortTicket
from models.season_pass import SeasonPass
from models.lift_ride import LiftRide

# Import storage
from storage.sqlite_backend import SQLiteBackend

# Import constants
from consts import (
    RESORTS, RESORT_WEIGHTS, RESORT_TZS, RESORT_LIFTS,
    SPEED_SETTINGS, MAX_TICKETS_PER_LOOP, MAX_PASSES_PER_LOOP, RIDE_MIN_INTERVAL, RIDE_MAX_INTERVAL, REST_MIN_INTERVAL,
    REST_MAX_INTERVAL
)

# How often to log summary (every N seconds)
SUMMARY_LOG_INTERVAL_SECONDS = 10


class DataGenerator:
    """Main data generator class"""

    def __init__(self):
        """Initialize the data generator"""
        self.start_time = None
        self.backend = SQLiteBackend()
        self.tickets_per_event_loop = 5
        self.tickets_to_season_pass_ratio = 20

        # Set random seed based on calendar date for deterministic behavior
        today = datetime.datetime.now(datetime.UTC).date()
        self.seed = today.year * 10000 + today.month * 100 + today.day
        random.seed(self.seed)

        # Tracking stats
        self.tickets_purchased = 0
        self.season_passes_purchased = 0
        self.lift_rides_generated = 0

        # Initialize empty lists for tickets and passes (no fixed size)
        self.resort_tickets = []
        self.season_passes = []

        # Import faker lazily if needed
        try:
            from faker import Faker
            self.faker = Faker()
            # Make faker deterministic too
            self.faker.seed_instance(self.seed)
        except ImportError:
            logger.warning("Faker not available. Using basic customer generation.")
            self.faker = None

        # Counter for deterministic ID generation
        self.id_counter = 0

        # Timing for summary logs
        self.last_summary_time = time.time()

        logger.info(f"Initialized data generator with seed: {self.seed}")

    # noinspection PyMethodMayBeStatic
    def _calculate_world_time_increment(self, world_time, current_time):
        """Calculate time increment based on simulation speed with safety limits"""
        speed = os.getenv("SPEED", "CHEETAH")

        # Calculate the time difference in seconds
        diff_seconds = (current_time - world_time).total_seconds()

        # Cap the difference to avoid extreme values (max 10 minutes of real time)
        diff_seconds = min(diff_seconds, 600)

        # Apply the speed multiplier
        multiplier = SPEED_SETTINGS.get(speed, SPEED_SETTINGS["CHEETAH"])

        # Calculate seconds to advance, with a reasonable cap
        seconds_to_advance = diff_seconds * multiplier

        # Cap maximum time advancement to 30 days to prevent overflow
        max_seconds = 30 * 24 * 60 * 60  # 30 days in seconds
        seconds_to_advance = min(seconds_to_advance, max_seconds)

        return datetime.timedelta(seconds=seconds_to_advance)

    def _generate_tickets(self, world_time):
        """Generate resort tickets"""
        # Select resorts with weighted probability
        resorts = random.choices(
            RESORTS, weights=RESORT_WEIGHTS, k=self.tickets_per_event_loop
        )

        # Generate and store tickets
        for resort in resorts:
            if self.faker:
                ticket = ResortTicket.generate(resort, world_time, self.faker, self.id_counter)
            else:
                # Basic ticket generation without faker
                ticket = ResortTicket(
                    resort=resort,
                    txid=f"TX-{self.id_counter}",
                    rfid=f"RFID-{self.id_counter}",
                    purchase_time=world_time.isoformat(),
                    price_usd=random.randrange(49, 199, 10),
                    _exp=world_time + datetime.timedelta(days=2),
                )
                ticket.expiration_time = ticket._exp.isoformat()

            self.id_counter += 1
            self.resort_tickets.append(ticket)
            self.backend.StoreResortTicket(ticket)
            self.tickets_purchased += 1

    def _generate_season_passes(self, world_time):
        """Generate season passes"""
        # Calculate how many season passes needed
        season_passes_needed = int(self.tickets_purchased / self.tickets_to_season_pass_ratio)

        # Generate new passes as needed
        while season_passes_needed > self.season_passes_purchased:
            if self.faker:
                season_pass = SeasonPass.generate(world_time, self.faker, self.id_counter)
            else:
                # Basic season pass generation without faker
                season_pass = SeasonPass(
                    txid=f"SP-{self.id_counter}",
                    rfid=f"RFID-SP-{self.id_counter}",
                    purchase_time=world_time.isoformat(),
                    price_usd=random.choice([1051, 783, 537, 407]),
                    _exp=world_time + datetime.timedelta(days=365),
                )
                season_pass.expiration_time = season_pass.exp.isoformat()

            self.id_counter += 1
            self.season_passes.append(season_pass)
            self.backend.StoreSeasonPass(season_pass)
            self.season_passes_purchased += 1

    # noinspection PyMethodMayBeStatic
    def _get_resort_time(self, world_time, resort):
        """Convert world time to resort local time"""
        resort_tz = RESORT_TZS[RESORTS.index(resort)]
        return world_time.astimezone(resort_tz)

    # noinspection PyMethodMayBeStatic
    def _is_resort_open(self, resort_time):
        """Check if resort is open at the given resort local time"""
        # Set resort opening time to 8:00 AM instead of 8:30 AM to increase lift ride generation
        open_time = resort_time.replace(hour=8, minute=0)
        close_time = resort_time.replace(hour=16, minute=0)
        return open_time <= resort_time < close_time

    def _process_lift_rides_for_item(self, item, world_time):
        """Process lift rides for a ticket or pass"""
        # Check if the item is riding today
        riding, resort = item.is_riding_today(world_time)
        if riding:
            # Get the resort local time
            resort_time = self._get_resort_time(world_time, resort)
            # Check if resort is open
            is_open = self._is_resort_open(resort_time)
            # Check if the ticket or pass needs a ride
            needs_ride = item.needs_ride(world_time)

            # Only generate rides during operating hours and when needed
            if is_open and needs_ride:
                lift_ride = LiftRide.generate(
                    item.rfid, resort, world_time, item._rider_skill, self.id_counter
                )
                self.id_counter += 1
                self.backend.StoreLiftRide(lift_ride)
                self.lift_rides_generated += 1

    def _process_lift_rides(self, world_time):
        """Process lift rides for active tickets and passes with balanced processing"""
        # Remove expired items first to reduce unnecessary processing
        self.resort_tickets = [t for t in self.resort_tickets if not t.is_expired(world_time)]
        self.season_passes = [sp for sp in self.season_passes if not sp.is_expired(world_time)]

        # Process only a subset of tickets each loop
        if len(self.resort_tickets) > 0:
            # Calculate how many tickets to process this loop
            tickets_to_process = min(len(self.resort_tickets), MAX_TICKETS_PER_LOOP)

            # Choose tickets to process randomly for a more balanced distribution
            selected_ticket_indices = random.sample(range(len(self.resort_tickets)), tickets_to_process)

            for idx in selected_ticket_indices:
                self._process_lift_rides_for_item(self.resort_tickets[idx], world_time)

        # Process only a subset of season passes each loop
        if len(self.season_passes) > 0:
            # Calculate how many passes to process this loop
            passes_to_process = min(len(self.season_passes), MAX_PASSES_PER_LOOP)

            # Choose passes to process randomly for a more balanced distribution
            selected_pass_indices = random.sample(range(len(self.season_passes)), passes_to_process)

            for idx in selected_pass_indices:
                self._process_lift_rides_for_item(self.season_passes[idx], world_time)

    def _log_summary(self, world_time):
        """Log a concise one-line summary of generation progress"""
        current_time = time.time()

        # Calculate generation rates
        seconds_elapsed = current_time - self.start_time
        tickets_per_sec = self.tickets_purchased / seconds_elapsed if seconds_elapsed > 0 else 0
        passes_per_sec = self.season_passes_purchased / seconds_elapsed if seconds_elapsed > 0 else 0
        rides_per_sec = self.lift_rides_generated / seconds_elapsed if seconds_elapsed > 0 else 0

        # Get memory usage
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_str = f" | {memory_mb:.1f}MB"
        except:
            memory_str = ""

        # Format world time
        world_time_str = world_time.strftime("%m/%d %H:%M")

        # Create one-line summary
        logger.info(f"[{world_time_str}] Generated: {self.tickets_purchased}T {self.season_passes_purchased}P {self.lift_rides_generated}R | "
                    f"Rate: {tickets_per_sec:.1f}T/s {passes_per_sec:.1f}P/s {rides_per_sec:.1f}R/s | "
                    f"Memory: {len(self.resort_tickets)}T {len(self.season_passes)}P{memory_str}")

    def event_loop(self):
        """Main event loop with simplified logging"""
        # Use a fixed time during resort open hours to ensure lift rides are generated
        current_date = datetime.datetime.now(datetime.UTC).date()
        world_time = datetime.datetime.combine(
            current_date,
            datetime.time(hour=14, minute=0),  # 2:00 PM UTC (8:00 AM Mountain, 7:00 AM Pacific)
            tzinfo=datetime.UTC
        )
        self.start_time = time.time()

        # Re-seed at the beginning of the loop to ensure consistent behavior
        # for people starting at the same calendar date
        random.seed(self.seed)
        if self.faker:
            self.faker.seed_instance(self.seed)

        try:
            logger.info(f"Starting data generation at {world_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")

            # Previous real time for time increment calculation
            previous_real_time = datetime.datetime.now(datetime.UTC)

            while True:
                # Generate season passes
                self._generate_season_passes(world_time)

                # Generate resort tickets
                self._generate_tickets(world_time)

                # Process lift rides
                self._process_lift_rides(world_time)

                # Advance world time using original logic but with safety limits
                current_time = datetime.datetime.now(datetime.UTC)
                time_increment = self._calculate_world_time_increment(previous_real_time, current_time)
                world_time += time_increment
                previous_real_time = current_time

                # Log summary every SUMMARY_LOG_INTERVAL_SECONDS seconds
                if current_time.timestamp() - self.last_summary_time >= SUMMARY_LOG_INTERVAL_SECONDS:
                    self._log_summary(world_time)
                    self.last_summary_time = current_time.timestamp()

        except KeyboardInterrupt:
            logger.info("Data generation interrupted")
            # Log final summary on exit
            self._log_summary(world_time)
        except Exception as e:
            logger.error(f"Error in data generation: {e}", exc_info=True)


def event_loop():
    """Main entry point for the generator, for backward compatibility"""
    generator = DataGenerator()
    generator.event_loop()


if __name__ == "__main__":
    event_loop()