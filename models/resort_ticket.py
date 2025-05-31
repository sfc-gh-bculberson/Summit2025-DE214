"""
Resort ticket model for ski resort data.
"""
import uuid

import datetime
import hashlib
import json
import random
from dataclasses import dataclass, field
from typing import Optional, List

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
    days: int = 1  # Number of days the ticket is valid for skiing
    txid: str = ""
    rfid: str = ""
    purchase_time: str = ""
    price_usd: int = 0
    expiration_time: str = "" # General expiration of the ticket media/offer
    customer: Customer = field(default_factory=Customer)

    # Internal tracking for lift rides
    _exp: datetime.datetime = field(default_factory=datetime.datetime.now)
    _last_ride_date_checked: Optional[datetime.datetime] = None # Tracks the last p_time this ticket's riding chance was evaluated
    _last_ride_date: Optional[datetime.datetime] = None # Tracks the p_time if decided to ride (for needs_ride)
    _last_lift_ridden: Optional[datetime.datetime] = None
    _rider_skill: float = 0.5  # 0-1 skill level for lift selection

    # Internal tracking or ticket usage
    _actual_days_used_list: List[datetime.date] = field(default_factory=list) # Stores unique dates skied
    _will_ride_decision_for_today: Optional[bool] = None # Stores the random choice for the current day

    @property
    def days_used_count(self) -> int:
        """Returns the number of unique days this ticket has been used."""
        return len(self._actual_days_used_list)

    @property
    def rider_skill(self) -> float:
        return self._rider_skill

    @classmethod
    def generate(cls, resort, p_time, faker, counter=0):
        """Generate a resort ticket with realistic parameters"""
        # Generate realistic values for ticket duration (days it can be used for skiing)
        ticket_ski_days = random.choices(TICKET_DAY_OPTIONS, weights=TICKET_DAY_WEIGHTS, k=1)[0]

        # Expiration of the ticket offer/media itself (e.g., must be used by X date or within Y days of purchase)
        # For simplicity, keeping original logic: valid for roughly twice its ski_days duration from purchase.
        # The actual ski day usage is controlled by _actual_days_used_list and self.days.
        exp_offset_days = ticket_ski_days * 2
        exp = p_time + datetime.timedelta(days=exp_offset_days)

        # Generate customer info
        customer = Customer.generate(faker)
        txid = str(uuid.uuid4())
        rfid = hex(random.getrandbits(96))

        # Price calculation with resort profiles
        profile = RESORT_PROFILES.get(resort, {'ticket_base_price': 100, 'weekend_multiplier': 1.5})
        base_price = profile.get('ticket_base_price', 100)

        # Apply weekend premium
        if p_time.weekday() >= 5:  # Saturday or Sunday
            base_price *= profile.get('weekend_multiplier', 1.5)

        # Apply random variations (10%) and multiply by ski_days
        price_usd = int(base_price * (0.9 + random.random() * 0.2) * ticket_ski_days)

        # Create ticket
        ticket = cls(
            resort=resort,
            days=ticket_ski_days, # This is the number of days it can be used for skiing
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
            "DAYS": self.days, # Number of ski days this ticket is for
            "RESORT": self.resort,
            "DAYS_USED": len(self._actual_days_used_list) # Number of unique days skied so far
        })

    def is_expired(self, p_time: datetime.datetime) -> bool:
        """Check if the ticket media/offer is expired (hard cutoff)."""
        return self._exp < p_time

    def is_riding_today(self, p_time: datetime.datetime) -> tuple[bool, Optional[str]]:
        """
        Determine if the ticket holder is riding today.
        Enforces 1-day ticket usage strictly to one calendar day.
        Tracks unique days used.
        """
        current_date = p_time.date()

        # 0. Check hard expiration first
        if self.is_expired(p_time):
            return False, None

        # 1. Enforce usage limits based on self.days (number of ski days allowed)
        # and specific 1-day ticket rule.
        current_days_used_count = len(self._actual_days_used_list)

        if self.days == 1: # Special handling for 1-day tickets
            if current_days_used_count > 0 and current_date not in self._actual_days_used_list:
                # It's a 1-day ticket, it has been used on a *different* day. Cannot use again.
                return False, None
            # If current_days_used_count is 0, or if it's >0 but current_date IS in the list, it's potentially usable.
        elif current_days_used_count >= self.days and current_date not in self._actual_days_used_list:
            # It's a multi-day ticket, all allowed ski days have been used on *other* distinct dates.
            # Cannot use on a new distinct date.
            return False, None

        # 2. Determine if the holder *chooses* to ride today (random chance, decide once per day)
        # This decision is independent of the strict usage/expiration rules above.
        if self._last_ride_date_checked is None or self._last_ride_date_checked.date() < current_date:
            self._last_ride_date_checked = p_time
            self._will_ride_decision_for_today = (random.random() <= DAILY_TICKET_RIDING_CHANCE)

        # 3. If they choose to ride (and passed previous checks):
        if self._will_ride_decision_for_today:
            # If we are here, it means:
            # - Ticket is not hard-expired.
            # - If 1-day ticket: either not used yet, or today is the day it was/is being used.
            # - If multi-day ticket: either has remaining ski days, or today is one of the already used ski days.

            # Record this day as used if it's a new unique ski day for this ticket
            if current_date not in self._actual_days_used_list:
                # Before adding, ensure we are not exceeding allowed days for multi-day tickets,
                # or adding a second day for a 1-day ticket (though step 1 should catch this).
                # This check is more of a safeguard.
                if len(self._actual_days_used_list) < self.days:
                    self._actual_days_used_list.append(current_date)
                else:
                    # Should not happen if logic in step 1 is correct.
                    # Means we decided to ride, but adding this date would exceed allowed ski days.
                    return False, None

            self._last_ride_date = p_time # For needs_ride logic: update the time of the *potential* last ride
            return True, self.resort

        return False, None

    def needs_ride(self, p_time: datetime.datetime) -> bool:
        """Determine if the rider needs a new lift ride (same logic as before)"""
        if random.random() <= 0.1:  # 10% chance of longer break
            wait_time = random.randrange(REST_MIN_INTERVAL, REST_MAX_INTERVAL)
        else:
            wait_time = random.randrange(RIDE_MIN_INTERVAL, RIDE_MAX_INTERVAL)

        if (self._last_lift_ridden is None or
                self._last_lift_ridden + datetime.timedelta(minutes=wait_time) < p_time):
            self._last_lift_ridden = p_time
            return True
        return False
