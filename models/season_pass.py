"""
Season pass model for ski resort data.
"""
import datetime
import hashlib
import random
import json
from dataclasses import dataclass, field
from typing import Optional

from models.customer import Customer
from consts import RESORTS, RESORT_WEIGHTS, RIDE_MIN, RIDE_MAX, REST_MIN, REST_MAX

# Chance of a season pass holder riding on any given day
CHANCE_OF_RIDING_SEASON_PASS = 0.05


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
    _last_ride_date: Optional[datetime.datetime] = None
    _last_lift_ridden: Optional[datetime.datetime] = None
    _last_resort: Optional[str] = None
    _rider_skill: float = 0.5  # 0-1 skill level for lift selection

    @classmethod
    def generate(cls, p_time, faker, counter=0):
        """Generate a season pass with realistic parameters"""
        # Generate realistic values
        exp = p_time + datetime.timedelta(days=365)

        # Generate customer info
        customer = Customer.generate(faker)

        # Generate deterministic TXID and RFID based on a combination of factors
        txid_seed = f"{p_time.isoformat()}-season-pass-{customer.name}-{counter}"
        txid_hash = hashlib.md5(txid_seed.encode()).hexdigest()
        txid = f"SP-{txid_hash[:8]}-{txid_hash[8:16]}"

        rfid_seed = f"{txid}-{counter}"
        rfid_hash = hashlib.md5(rfid_seed.encode()).hexdigest()
        rfid = f"RFID-SP-{rfid_hash[:8]}-{rfid_hash[8:16]}"

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
        rand = random.random() * total_weight
        cumulative_weight = 0
        price_usd = price_options[0]["price"]  # Default

        for option in price_options:
            cumulative_weight += option["weight"]
            if rand <= cumulative_weight:
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

    def toJSON(self):
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
        })

    def isExpired(self, p_time):
        """Check if the pass is expired"""
        return self._exp < p_time

    def isRidingToday(self, p_time):
        """Determine if the pass holder is riding today"""
        # Only check once per day
        if (self._last_ride_date_checked is None or
                self._last_ride_date_checked.date() < p_time.date()):

            self._last_ride_date_checked = p_time

            # Chance to ride today
            if random.random() <= CHANCE_OF_RIDING_SEASON_PASS:
                self._last_resort = random.choices(RESORTS, weights=RESORT_WEIGHTS, k=1)[0]
                self._last_ride_date = p_time
                return True, self._last_resort

            return False, None

        # Already decided for today
        if (self._last_ride_date is not None and
                self._last_ride_date.date() == p_time.date()):
            return True, self._last_resort

        return False, None

    def needsRide(self, p_time):
        """Determine if the rider needs a new lift ride"""
        # Time between rides - normal or rest break
        if random.random() <= 0.1:  # 10% chance of longer break
            wait_time = random.randrange(REST_MIN, REST_MAX)
        else:
            wait_time = random.randrange(RIDE_MIN, RIDE_MAX)

        # First ride or enough time has passed
        if (self._last_lift_ridden is None or
                self._last_lift_ridden + datetime.timedelta(minutes=wait_time) < p_time):

            self._last_lift_ridden = p_time
            return True

        return False