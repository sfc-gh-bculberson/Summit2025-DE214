import rapidjson as json
import optional_faker as _
import uuid
import random

from faker import Faker
from datetime import date, datetime

fake = Faker()
resorts = ["Vail", "Beaver Creek", "Breckenridge", "Keystone", "Crested Butte", "Park City", "Heavenly", "Northstar",
           "Kirkwood", "Whistler Blackcomb", "Perisher", "Falls Creek", "Hotham", "Stowe", "Mount Snow", "Okemo",
           "Hunter Mountain", "Mount Sunapee", "Attitash", "Wildcat", "Crotched", "Stevens Pass", "Liberty", "Roundtop", 
           "Whitetail", "Jack Frost", "Big Boulder", "Alpine Valley", "Boston Mills", "Brandywine", "Mad River",
           "Hidden Valley", "Snow Creek", "Wilmot", "Afton Alps" , "Mt. Brighton", "Paoli Peaks"]    


def get_lift_ticket():
    global resorts, fake
    state = fake.state_abbr()
    lift_ticket = {'TXID': str(uuid.uuid4()),
                   'RFID': hex(random.getrandbits(96)),
                   'RESORT': fake.random_element(elements=resorts),
                   'PURCHASE_TIME': datetime.utcnow().isoformat(),
                   'EXPIRATION_TIME': date(2023, 6, 1).isoformat(),
                   'DAYS': fake.random_int(min=1, max=7),
                   'NAME': fake.name(),
                   'ADDRESS': fake.none_or({'STREET_ADDRESS': fake.street_address(), 
                                             'CITY': fake.city(), 'STATE': state, 
                                             'POSTALCODE': fake.postalcode_in_state(state)}),
                   'PHONE': fake.none_or(fake.phone_number()),
                   'EMAIL': fake.none_or(fake.email()),
                   'EMERGERCY_CONTACT' : fake.none_or({'NAME': fake.name(), 'PHONE': fake.phone_number()}),
    }
    d = json.dumps(lift_ticket)
    return d
