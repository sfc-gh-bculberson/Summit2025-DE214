import rapidjson as json
import optional_faker as _
import uuid
import random
import sqlite3
import psutil
import datetime
import os

from faker import Faker

from generate_rides import generate_rides


fake = Faker()
# TODO ADD MORE RESORTS?
resorts = ["Vail", "Beaver Creek", "Breckenridge", "Keystone", "Heavenly"]    
weights = [0.25, 0.2, 0.25, 0.2, 0.1]


def generate_resort_ticket(cur, con, p_time):
    global resorts, fake
    state = fake.state_abbr()
    days = fake.random_int(min=1, max=7)
    resort = random.choices(population=resorts, weights=weights, k=1)[0]
    exp = p_time + datetime.timedelta(days=days*2)
    resort_ticket = {'TXID': str(uuid.uuid4()),
                   'RFID': hex(random.getrandbits(96)),
                   'RESORT': resort,
                   'PURCHASE_TIME': p_time.isoformat(),  
                   'PRICE_USD': random.randrange(49,199,10)*days,         
                   'DAYS': days,
                   'EXPIRATION_TIME': exp.isoformat(),
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
    return (resort_ticket['RFID'], exp, resort_ticket['RESORT'])


def generate_season_pass(cur, con, p_time):
    global fake
    state = fake.state_abbr()
    exp = p_time + datetime.timedelta(days=365)
    season_pass = {'TXID': str(uuid.uuid4()),
                   'RFID': hex(random.getrandbits(96)),
                   'PURCHASE_TIME': p_time.isoformat(),
                   'PRICE_USD': random.choices([1051, 783, 537, 407], weights=[0.4, 0.5, 0.05, 0.05], k=1)[0],
                   'EXPIRATION_TIME': exp.isoformat(),
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
    return (season_pass["RFID"], exp)
            

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
    speed = os.getenv("SPEED")
    world_time = datetime.datetime.now(datetime.UTC)
    passes_processed = world_time
    season_passes = []
    tickets = []
    while(True):
        # Keep at least 1 gb on disk free
        if psutil.disk_usage('.').free > 1e+9:
            # SIM CITY SPEEDS 
            # TURTLE: 1 day == 12 min, LLAMA: 1 day == 3 min, CHEETAH: 1 day == 90 sec
            season_pass = generate_season_pass(cur, con, world_time)
            season_passes.append(season_pass)
            for _ in range(tickets_to_passes):
                ticket = generate_resort_ticket(cur, con, world_time)
                tickets.append(ticket)            
            # did a full day pass?
            if world_time - passes_processed > datetime.timedelta(days=1):
                # did season pass holders ski today?
                active_season_passes = []
                for (rfid, exp) in season_passes:
                    # 5% chance of skiing on purchase day
                    if exp > world_time:
                        active_season_passes.append((rfid, exp)) 
                        if random.random() > 0.95:
                            resort = random.choices(population=resorts, weights=weights, k=1)[0]
                            generate_rides(rfid, resort, world_time - datetime.timedelta(hours=-7), cur, con)
                season_passes = active_season_passes
                active_tickets = []
                for (rfid, exp, resort) in tickets:
                    if exp > world_time:
                        active_tickets.append((rfid, exp, resort))
                        # 55% chance of skiing on purchase day
                        if random.random() > 0.55:
                            generate_rides(rfid, resort, world_time - datetime.timedelta(hours=-7), cur, con)
                tickets = active_tickets
                passes_processed = world_time
                
            # progress world time
            diff = datetime.datetime.now(datetime.UTC) - world_time
            if speed == "TURTLE":
                world_time = world_time + datetime.timedelta(seconds=diff.seconds*120)
            elif speed == "LLAMA":
                world_time = world_time + datetime.timedelta(seconds=diff.seconds*480)
            else:
                world_time = world_time + datetime.timedelta(seconds=diff.seconds*960)


if __name__ == "__main__":
    main()