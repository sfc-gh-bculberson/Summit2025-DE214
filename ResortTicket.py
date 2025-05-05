import datetime
import json
import uuid
import random
import optional_faker

from consts import *
from faker import Faker

fake = Faker()

day_options = [1, 2, 3, 4, 5, 6, 7]
day_weights = [0.35, 0.35, 0.1, 0.05, 0.05, 0.05, 0.05]
chance_of_riding = 0.45


class ResortTicket:
    def __init__(self, resort, p_time):
        self.state = fake.state_abbr()
        self.days = random.choices(day_options, weights=day_weights, k=1)[0]
        self.exp = p_time + datetime.timedelta(days=self.days * 2)
        self.txid = str(uuid.uuid4())
        self.rfid = hex(random.getrandbits(96))
        self.resort = resort
        self.purchase_time = p_time.isoformat()
        self.price_usd = random.randrange(49, 199, 10) * self.days
        self.expiration_time = self.exp.isoformat()
        self.name = fake.name()
        self.address = fake.none_or(
            {
                "STREET_ADDRESS": fake.street_address(),
                "CITY": fake.city(),
                "STATE": self.state,
                "POSTALCODE": fake.postalcode_in_state(self.state),
            }
        )
        self.phone = fake.none_or(fake.phone_number())
        self.email = fake.none_or(fake.email())
        self.emergency_contact = fake.none_or(
            {"NAME": fake.name(), "PHONE": fake.phone_number()}
        )

    def __str__(self):
        return f"{self.txid}({self.name})"

    def toJSON(self):
        return json.dumps(
            {
                "TXID": self.txid,
                "RFID": self.rfid,
                "PURCHASE_TIME": self.purchase_time,
                "PRICE_USD": self.price_usd,
                "EXPIRATION_TIME": self.expiration_time,
                "NAME": self.name,
                "ADDRESS": getattr(self, "address", None),
                "PHONE": self.phone,
                "EMAIL": self.email,
                "EMERGENCY_CONTACT": getattr(self, "emergency_contact", None),
                "DAYS": self.days,
                "RESORT": self.resort,
            }
        )

    def isExpired(self, p_time):
        return self.exp < p_time

    def isRidingToday(self, p_time):
        if (
            getattr(self, "last_ride_date_checked", None) is None
            or self.last_ride_date_checked.date() < p_time.date()
        ):
            self.last_ride_date_checked = p_time
            if random.random() > (1 - chance_of_riding):
                self.last_ride_date = p_time
                return True, self.resort
            else:
                return False, None
        else:
            if (
                getattr(self, "last_ride_date", None) is not None
                and self.last_ride_date.date() == p_time.date()
            ):
                return True, self.resort
            else:
                return False, None

    def needsRide(self, p_time):
        last_ride = random.randrange(RIDE_MIN, RIDE_MAX)
        # longer break
        if random.choices([True, False], weights=[0.1, 0.9], k=1)[0]:
            last_ride = random.randrange(REST_MIN, REST_MAX)
        if (
            getattr(self, "last_lift_ridden", None) is not None
            and self.last_lift_ridden + datetime.timedelta(minutes=last_ride) < p_time
        ):
            self.last_lift_ridden = p_time
            return True
        elif getattr(self, "last_lift_ridden", None) is None:
            self.last_lift_ridden = p_time
            return True
