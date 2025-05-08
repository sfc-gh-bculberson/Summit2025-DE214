#!/usr/bin/env python

import os
import datetime
import random
import logging

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

# Import constants (keep your existing file)
from consts import (
    RESORTS, RESORT_WEIGHTS, RESORT_TZS, RESORT_LIFTS,
    RIDE_MIN, RIDE_MAX, REST_MIN, REST_MAX
)

class DataGenerator:
    """Main data generator class with original memory management behavior"""

    def __init__(self):
        """Initialize the data generator"""
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

        logger.info(f"Initialized data generator with seed: {self.seed} (based on calendar date)")

    def _calculate_world_time_increment(self, world_time, current_time):
        """Calculate time increment based on simulation speed with safety limits"""
        speed = os.getenv("SPEED", "CHEETAH")

        # Calculate the time difference in seconds
        diff_seconds = (current_time - world_time).total_seconds()

        # Cap the difference to avoid extreme values (max 10 minutes of real time)
        diff_seconds = min(diff_seconds, 600)

        # Apply the speed multiplier
        if speed == "TURTLE":
            # 1 day == 12 min → 120x multiplier
            multiplier = 120
        elif speed == "LLAMA":
            # 1 day == 3 min → 480x multiplier
            multiplier = 480
        else:  # Default to CHEETAH
            # 1 day == 90 sec → 960x multiplier
            multiplier = 960

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
                season_pass.expiration_time = season_pass._exp.isoformat()

            self.id_counter += 1
            self.season_passes.append(season_pass)
            self.backend.StoreSeasonPass(season_pass)
            self.season_passes_purchased += 1

    def _process_lift_rides(self, world_time):
        """Process lift rides for active tickets and passes"""
        # Process regular tickets
        for ticket in self.resort_tickets:
            riding, resort = ticket.isRidingToday(world_time)
            if riding:
                # Get the timezone for the resort
                resort_tz = RESORT_TZS[RESORTS.index(resort)]
                resort_time = world_time.astimezone(resort_tz)

                # Resort hours - 8:30 AM to 4:00 PM in resort's timezone
                open_time = resort_time.replace(hour=8, minute=30)
                close_time = resort_time.replace(hour=16, minute=0)

                # Only generate rides during operating hours
                if open_time <= resort_time < close_time and ticket.needsRide(world_time):
                    lift_ride = LiftRide.generate(
                        ticket.rfid, resort, world_time, ticket._rider_skill, self.id_counter
                    )
                    self.id_counter += 1
                    self.backend.StoreLiftRide(lift_ride)

        # Remove expired tickets - using original list comprehension approach
        self.resort_tickets = [t for t in self.resort_tickets if not t.isExpired(world_time)]

        # Process season passes
        for season_pass in self.season_passes:
            riding, resort = season_pass.isRidingToday(world_time)
            if riding:
                # Get the timezone for the resort
                resort_tz = RESORT_TZS[RESORTS.index(resort)]
                resort_time = world_time.astimezone(resort_tz)

                # Resort hours - 8:30 AM to 4:00 PM in resort's timezone
                open_time = resort_time.replace(hour=8, minute=30)
                close_time = resort_time.replace(hour=16, minute=0)

                # Only generate rides during operating hours
                if open_time <= resort_time < close_time and season_pass.needsRide(world_time):
                    lift_ride = LiftRide.generate(
                        season_pass.rfid, resort, world_time, season_pass._rider_skill, self.id_counter
                    )
                    self.id_counter += 1
                    self.backend.StoreLiftRide(lift_ride)

        # Remove expired passes - using original list comprehension approach
        self.season_passes = [sp for sp in self.season_passes if not sp.isExpired(world_time)]

    def event_loop(self):
        """Main event loop with original memory behavior"""
        # Use the actual current time as in the original code
        world_time = datetime.datetime.now(datetime.UTC)

        # Re-seed at the beginning of the loop to ensure consistent behavior
        # for people starting at the same calendar date
        random.seed(self.seed)
        if self.faker:
            self.faker.seed_instance(self.seed)

        try:
            logger.info(f"Starting data generation loop with seed {self.seed}")

            # Previous real time for time increment calculation
            previous_real_time = datetime.datetime.now(datetime.UTC)

            while True:
                # Get current real time to calculate simulation speed
                current_time = datetime.datetime.now(datetime.UTC)

                # Generate season passes
                self._generate_season_passes(world_time)

                # Generate resort tickets
                self._generate_tickets(world_time)

                # Process lift rides
                self._process_lift_rides(world_time)

                # Advance world time using original logic but with safety limits
                time_increment = self._calculate_world_time_increment(previous_real_time, current_time)
                world_time += time_increment
                previous_real_time = current_time

                # Log status periodically
                if self.tickets_purchased % 100 == 0:
                    logger.info(
                        f"Generated: {self.tickets_purchased} tickets, "
                        f"{self.season_passes_purchased} season passes, "
                        f"Active items in memory: {len(self.resort_tickets)} tickets, "
                        f"{len(self.season_passes)} passes"
                    )

        except KeyboardInterrupt:
            logger.info("Data generation interrupted")
        except Exception as e:
            logger.error(f"Error in data generation: {e}", exc_info=True)


def event_loop():
    """Main entry point for the generator, for backward compatibility"""
    generator = DataGenerator()
    generator.event_loop()


if __name__ == "__main__":
    event_loop()