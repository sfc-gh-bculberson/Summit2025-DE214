"""
Resort ticket model for ski resort data.
"""
import datetime
import hashlib
import json
import random
from dataclasses import dataclass, field
from typing import Optional

from models.customer import Customer
from consts import (
    TICKET_DAY_OPTIONS, TICKET_DAY_WEIGHTS, DAILY_TICKET_RIDING_CHANCE,
    RIDE_MIN_INTERVAL, RIDE_MAX_INTERVAL, REST_MIN_INTERVAL, REST_MAX_INTERVAL,
    RESORT_PROFILES
)

@dataclass
class ResortTicket:
    """Resort ticket with realistic properties"""
    resort: str
    days: int = 1
    txid: str = ""
    rfid: str = ""
    purchase_time: str = ""
    price_usd: int = 0
    expiration_time: str = ""
    customer: Customer = field(default_factory=Customer)

    # Internal tracking for lift rides
    _exp: datetime.datetime = field(default_factory=datetime.datetime.now)
    _last_ride_date_checked: Optional[datetime.datetime] = None
    _last_ride_date: Optional[datetime.datetime] = None
    _last_lift_ridden: Optional[datetime.datetime] = None
    _rider_skill: float = 0.5  # 0-1 skill level for lift selection

    @classmethod
    def generate(cls, resort, p_time, faker, counter=0):
        """Generate a resort ticket with realistic parameters"""
        # Generate realistic values
        days = random.choices(TICKET_DAY_OPTIONS, weights=TICKET_DAY_WEIGHTS, k=1)[0]
        exp = p_time + datetime.timedelta(days=days * 2)

        # Generate customer info
        customer = Customer.generate(faker)

        # Generate deterministic TXID and RFID based on a combination of factors
        txid_seed = f"{p_time.isoformat()}-{resort}-{customer.name}-{counter}"
        txid_hash = hashlib.md5(txid_seed.encode()).hexdigest()
        txid = f"TX-{txid_hash[:8]}-{txid_hash[8:16]}"

        rfid_seed = f"{txid}-{counter}"
        rfid_hash = hashlib.md5(rfid_seed.encode()).hexdigest()
        rfid = f"RFID-{rfid_hash[:8]}-{rfid_hash[8:16]}"

        # Price calculation with resort profiles
        profile = RESORT_PROFILES.get(resort, {'ticket_base_price': 100, 'weekend_multiplier': 1.5})
        base_price = profile.get('ticket_base_price', 100)

        # Apply weekend premium
        if p_time.weekday() >= 5:  # Saturday or Sunday
            base_price *= profile.get('weekend_multiplier', 1.5)

        # Apply random variations (10%) and multiply by days
        price_usd = int(base_price * (0.9 + random.random() * 0.2) * days)

        # Create ticket
        ticket = cls(
            resort=resort,
            days=days,
            txid=txid,
            rfid=rfid,
            purchase_time=p_time.isoformat(),
            price_usd=price_usd,
            expiration_time=exp.isoformat(),
            customer=customer,
            _exp=exp,
            _rider_skill=random.random()  # Random skill level
        )

        return ticket

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
            "DAYS": self.days,
            "RESORT": self.resort,
        })

    def is_expired(self, p_time):
        """Check if the ticket is expired"""
        return self._exp < p_time

    def is_riding_today(self, p_time):
        """Determine if the ticket holder is riding today"""
        # Only check once per day
        if (self._last_ride_date_checked is None or
                self._last_ride_date_checked.date() < p_time.date()):

            self._last_ride_date_checked = p_time

            # Chance to ride today
            if random.random() <= DAILY_TICKET_RIDING_CHANCE:
                self._last_ride_date = p_time
                return True, self.resort

            return False, None

        # Already decided for today
        if (self._last_ride_date is not None and
                self._last_ride_date.date() == p_time.date()):
            return True, self.resort

        return False, None

    def needs_ride(self, p_time):
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