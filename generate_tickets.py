import rapidjson as json
import optional_faker as _
import uuid
import random
import sqlite3
import psutil
import datetime

from time import sleep
from faker import Faker

from generate_rides import generate_rides


fake = Faker()
# TODO ADD MORE RESORTS?
resorts = ["Vail", "Beaver Creek", "Breckenridge", "Keystone", "Heavenly"]    
weights = [0.25, 0.2, 0.25, 0.2, 0.1]


def generate_resort_ticket(cur, con):
    global resorts, fake
    state = fake.state_abbr()
    days = fake.random_int(min=1, max=7)
    p_time = datetime.datetime.now(datetime.UTC)
    resort = random.choices(population=resorts, weights=weights, k=1)[0]
    resort_ticket = {'TXID': str(uuid.uuid4()),
                   'RFID': hex(random.getrandbits(96)),
                   'RESORT': resort,
                   'PURCHASE_TIME': p_time.isoformat(),                   
                   'DAYS': days,
                   'EXPIRATION_TIME': (p_time+datetime.timedelta(days=days*2)).isoformat(),
                   'NAME': fake.name(),
                   'ADDRESS': fake.none_or({'STREET_ADDRESS': fake.street_address(), 
                                             'CITY': fake.city(), 'STATE': state, 
                                             'POSTALCODE': fake.postalcode_in_state(state)}),
                   'PHONE': fake.none_or(fake.phone_number()),
                   'EMAIL': fake.none_or(fake.email()),
                   'EMERGERCY_CONTACT' : fake.none_or({'NAME': fake.name(), 'PHONE': fake.phone_number()}),
    }
    cur.execute("INSERT INTO resort_ticket (data) VALUES (?)", (json.dumps(resort_ticket),))
    con.commit()
    for day in range(0, days):
        if random.random() > 0.55: # TODO DISTRIBUTE BETTER FOR REPORT?
            generate_rides(resort_ticket["RFID"], resort, p_time+datetime.timedelta(days=day), cur, con)


def generate_season_pass(cur, con):
    global fake
    state = fake.state_abbr()
    p_time = datetime.datetime.now(datetime.UTC)
    season_pass = {'TXID': str(uuid.uuid4()),
                   'RFID': hex(random.getrandbits(96)),
                   'PURCHASE_TIME': p_time.isoformat(),                   
                   'EXPIRATION_TIME': (p_time+datetime.timedelta(days=365)).isoformat(),
                   'NAME': fake.name(),
                   'ADDRESS': fake.none_or({'STREET_ADDRESS': fake.street_address(), 
                                             'CITY': fake.city(), 'STATE': state, 
                                             'POSTALCODE': fake.postalcode_in_state(state)}),
                   'PHONE': fake.none_or(fake.phone_number()),
                   'EMAIL': fake.none_or(fake.email()),
                   'EMERGERCY_CONTACT' : fake.none_or({'NAME': fake.name(), 'PHONE': fake.phone_number()}),
    }
    cur.execute("INSERT INTO season_pass (data) VALUES (?)", (json.dumps(season_pass),))
    con.commit()
    # TODO DISTRIBUTE BETTER FOR REPORT?
    for day in range(0, 365):
        if random.random() > 0.95:
            resort = random.choices(population=resorts, weights=weights, k=1)[0]
            generate_rides(season_pass["RFID"], resort, p_time+datetime.timedelta(days=day), cur, con)
            

def main():
    con = sqlite3.connect("data.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS season_pass (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS resort_ticket (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS lift_ride (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)")
    con.commit()
    tickets_to_passes = 10
    while(True):
        # Keep at least 1 gb on disk free
        if psutil.disk_usage('.').free > 1e+9:
            generate_season_pass(cur, con)
            for _ in range(tickets_to_passes):
                generate_resort_ticket(cur, con)
            # wait random time up to 200ms
            sleep(random.uniform(0,0.2))


if __name__ == "__main__":
    main()