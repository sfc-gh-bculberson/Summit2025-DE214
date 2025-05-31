"""
Season pass model for ski resort data.
"""
import uuid

import datetime
import hashlib
import json
import random
from dataclasses import dataclass, field
from typing import Optional, List

from consts import (
    RESORTS, RESORT_WEIGHTS, SEASON_PASS_RIDING_CHANCE,
    RIDE_MIN_INTERVAL, RIDE_MAX_INTERVAL, REST_MIN_INTERVAL, REST_MAX_INTERVAL
)
from models.customer import Customer

@dataclass
class SeasonPass:
    """Season pass with realistic properties"""
    txid: str = ""
    rfid: str = ""
    purchase_time: str = ""
    price_usd: int = 0
    expiration_time: str = ""
    customer: Customer = field(default_factory=Customer)

    # Internal tracking for lift rides
    _exp: datetime.datetime = field(default_factory=datetime.datetime.now)
    _last_ride_date_checked: Optional[datetime.datetime] = None
    _last_ride_date: Optional[datetime.datetime] = None # Stores p_time if decided to ride (for needs_ride)
    _last_lift_ridden: Optional[datetime.datetime] = None
    _last_resort: Optional[str] = None
    _rider_skill: float = 0.5  # 0-1 skill level for lift selection

    # Internal tracking of pass usage
    _actual_days_skied_list: List[datetime.date] = field(default_factory=list) # Stores unique dates skied
    _will_ride_decision_for_today: Optional[bool] = None # Stores the random choice for the current day

    @property
    def days_skied_count(self) -> int:
        """Returns the number of unique days this pass has been used."""
        return len(self._actual_days_skied_list)

    @property
    def rider_skill(self) -> float:
        return self._rider_skill

    @classmethod
    def generate(cls, p_time, faker, counter=0):
        """Generate a season pass with realistic parameters"""
        exp = p_time + datetime.timedelta(days=365) # Season passes typically valid for a season/year

        # Generate customer info
        customer = Customer.generate(faker)
        txid = str(uuid.uuid4())
        rfid = hex(random.getrandbits(96))

        # Season pass pricing - more realistic pricing with tiers
        price_options = [
            {"price": 1051, "weight": 0.4},
            {"price": 783, "weight": 0.5},
            {"price": 537, "weight": 0.05},
            {"price": 407, "weight": 0.05}
        ]
        # Calculate total weight
        total_weight = sum(option["weight"] for option in price_options)

        # Select price based on weights
        rand_val = random.random() * total_weight
        cumulative_weight = 0
        price_usd = price_options[0]["price"]
        for option in price_options:
            cumulative_weight += option["weight"]
            if rand_val <= cumulative_weight:
                price_usd = option["price"]
                break

        # Create pass
        season_pass = cls(
            txid=txid,
            rfid=rfid,
            purchase_time=p_time.isoformat(),
            price_usd=price_usd,
            expiration_time=exp.isoformat(),
            customer=customer,
            _exp=exp,
            _rider_skill=random.random()  # Random skill level
        )

        return season_pass

    def to_json(self):
        """Convert to JSON compatible dictionary"""
        return json.dumps({
            "TXID": self.txid,
            "RFID": self.rfid,
            "PURCHASE_TIME": self.purchase_time,
            "PRICE_USD": self.price_usd,
            "EXPIRATION_TIME": self.expiration_time,
            "NAME": self.customer.name,
            "ADDRESS": self.customer.address,
            "PHONE": self.customer.phone,
            "EMAIL": self.customer.email,
            "EMERGENCY_CONTACT": self.customer.emergency_contact,
            "DAYS_USED": len(self._actual_days_skied_list) # Number of unique days skied so far
        })

    def is_expired(self, p_time: datetime.datetime) -> bool:
        """Check if the pass is expired."""
        return self._exp < p_time

    def is_riding_today(self, p_time: datetime.datetime) -> tuple[bool, Optional[str]]:
        """
        Determine if the pass holder is riding today.
        Tracks unique days skied.
        """
        current_date = p_time.date()

        if self.is_expired(p_time):
            return False, None

        # Determine if the holder *chooses* to ride today (random chance, decide once per day)
        if self._last_ride_date_checked is None or self._last_ride_date_checked.date() < current_date:
            self._last_ride_date_checked = p_time
            self._will_ride_decision_for_today = (random.random() <= SEASON_PASS_RIDING_CHANCE)
            if self._will_ride_decision_for_today:
                # Choose resort for the day only if they decide to ride
                self._last_resort = random.choices(RESORTS, weights=RESORT_WEIGHTS, k=1)[0]
            else:
                self._last_resort = None # Clear last resort if not riding

        if self._will_ride_decision_for_today:
            # Record this day as skied if it's a new unique ski day
            if current_date not in self._actual_days_skied_list:
                self._actual_days_skied_list.append(current_date)

            self._last_ride_date = p_time # For needs_ride logic
            return True, self._last_resort # self._last_resort was set when decision was made

        return False, None

    def needs_ride(self, p_time: datetime.datetime) -> bool:
        """Determine if the rider needs a new lift ride

        Uses RIDE_MIN_INTERVAL/RIDE_MAX_INTERVAL for normal ride intervals,
        and REST_MIN_INTERVAL/REST_MAX_INTERVAL for occasional longer breaks.

        Args:
            p_time: Current world time

        Returns:
            True if a new lift ride should be generated, False otherwise
        """

        # Determine time between rides
        # 10% chance of a longer break (REST_MIN_INTERVAL to REST_MAX_INTERVAL)
        # 90% chance of a normal interval (RIDE_MIN_INTERVAL to RIDE_MAX_INTERVAL)
        if random.random() <= 0.1:  # 10% chance of longer break
            wait_time = random.randrange(REST_MIN_INTERVAL, REST_MAX_INTERVAL)
        else:
            wait_time = random.randrange(RIDE_MIN_INTERVAL, RIDE_MAX_INTERVAL)

        # First ride or enough time has passed
        if (self._last_lift_ridden is None or
                self._last_lift_ridden + datetime.timedelta(minutes=wait_time) < p_time):
            self._last_lift_ridden = p_time
            return True

        return False

    @property
    def exp(self):
        return self._exp