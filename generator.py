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
    lift_ticket = {'txid': str(uuid.uuid4()),
                   'rfid': hex(random.getrandbits(96)),
                   'resort': fake.random_element(elements=resorts),
                   'purchase_time': datetime.utcnow().isoformat(),
                   'expiration_time': date(2023, 6, 1).isoformat(),
                   'days': fake.random_int(min=1, max=7),
                   'name': fake.name(),
                   'address': fake.none_or({'street_address': fake.street_address(), 
                                             'city': fake.city(), 'state': state, 
                                             'postalcode': fake.postalcode_in_state(state)}),
                   'phone': fake.none_or(fake.phone_number()),
                   'email': fake.none_or(fake.email()),
                   'emergency_contact' : fake.none_or({'name': fake.name(), 'phone': fake.phone_number()}),
    }
    d = json.dumps(lift_ticket) + '\n'
    return d
