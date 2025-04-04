import json
import random
import uuid
import datetime

from faker import Faker

fake = Faker()


vail_lifts = [
    "Eagle Bahn Gondola",
    "Gondola One",
    "Game Creek Express",
    "Northwoods Express",
    "Avanti Express",
    "Mountain Top Express",
    "Sun Down Express",
    "Sun Up Express",
    "High Noon Express",
    "Sourdough Express",
    "Highline Express",
    "Pete's Express",
    "Tea Cup Express",
    "Skyline Express",
    "Earl's Express",
    "Riva Bahn Express",
    "Wildwood Express",
    "Pride Express",
    "Born Free Express",
    "Orient Express",
    "Cascade Village",
    "Golden Peak",
    "Little Eagle",
    "Golden Peak",
    "Wapiti",
    "Mongolia",
    "Black Forest",
    "Elvis Bahn",
    "Rip's Ride Carpet",
    "Adventure Ridge",
    "Thunder Cat Carpet",
    "Golden Peak Carpet",
    "Lightning Coyote",
    "Lionshead Magic Carpet",
]
beaver_lifts = [
    "Haymeadow Express Gondola",
    "Riverfront Express",
    "Centennial Express",
    "McCoy Park Express Chairlift",
    "Red Buffalo Express 5",
    "Rose Bowl Express",
    "Larkspur Express",
    "Lower Beaver Creek Mountain Express",
    "Upper Beaver Creek Mountain Express",
    "Birds of Prey Express",
    "Cinch Express",
    "Bachelor Gulch Express",
    "Strawberry Park Express",
    "Grouse Mountain Express",
    "Arrow Bahn Express",
    "Reunion Chairlift",
    "Elkhorn",
    "Highlands",
    "Magic Carpet Beaver Creek",
    "Ritz Bahn",
    "Wagon Train",
    "Jitterbug",
    "Bibber Bahn",
    "Trail Rider",
    "Snowflake",   
]
breckenridge_lifts = [
    "BreckConnect Gondola",
    "Falcon SuperChair",
    "Colorado SuperChair",
    "Kensho SuperChair",
    "Independance SuperChair",
    "QuickSilver Super 6",
    "Five SuperChair",
    "Rip's Ride",
    "Freedom SuperChair",
    "Imperial Express SuperChair",
    "Peak 8 Super Connect",
    "Mercury SuperChair",
    "Rocky Mountain SuperChair",
    "Beaver Run SuperChair",
    "Zendo Chair",
    "A-Chair",
    "Snowflake",
    "C-Chair",
    "E-Chair",
    "6-Chair",
    "Horseshoe Bowl T-Bar",
    "Trygve's Platter",
    "Eldora Platter",
    "Camelback Platter",
    "El Dorado Tow",
    "Castle Carpet 2",
    "Village Carpet B",
    "El Dorado Carpet C",
    "El Dorado Carpet D",
    "Ski and Ride Carpet 1",
    "Ski and Ride Carpet 4",
    "Village Carpet A",
    "Ski and Ride Carpet 2",
    "Ski and Ride Carpet 3",
    "Castle Carpet 1"
]
keysone_lifts = [
    "River Run Gondola",
    "Outpost Gondola",
    "Bergman Bowl Express",
    "Peru Express",
    "Montezuma Express",
    "Ruby Express",
    "Santiago Express",
    "Summit Express",
    "Outback Express",
    "Wayback",
    "Ranger",
    "Discovery",
    "A51 Lift",
    "Mid-Station Ski School",
    "Kokomo",
    "Double Barrel 2",
    "Sun Kid",
    "Cadillac",
    "Double Barrel 1",
    "Triangle",
    "Tubing Hill",
]
heavenly_lifts = [
    "Aerial Tram",
    "Heavenly Gondola",
    "Powderbowl Express",
    "Tamarack Express",
    "North Bowl Express",
    "Olympic Express",
    "Canyon Express",
    "Stagecoach Express",
    "Gunbarrel Express",
    "Dipper Express",
    "Comet Express",
    "Sky Express",
    "Big Easy",
    "Galaxy",
    "First Ride",
    "Groove",
    "Boulder",
    "Patsy's",
    "World Cup",
    "Mott Canyon",
    "Mitey-Mite Forest",
    "Pioneer Mitey Mite Tow",
    "DMZ Carpet",
    "Enchanted Carpet",
    "Boulder Carpet",
    "Bear Cave Carpet",
    "Tubing Lift",    
]
# TODO ADD MORE RESORTS?

resort_lifts = {"Vail": vail_lifts, "Beaver Creek": beaver_lifts, "Breckenridge": breckenridge_lifts, "Keystone": keysone_lifts, "Heavenly": heavenly_lifts}


def generate_rides(rfid, resort, rtime, cur, con):
    if not resort in resort_lifts:
        return
    lifts = resort_lifts[resort]
    # TODO BETTER DISTRIBUTION?
    lifts_ridden = fake.random_int(1,20)
    lift_rides = random.choices(lifts, k=lifts_ridden)
    for lift in lift_rides:
        rtime = rtime + datetime.timedelta(minutes=random.randint(1, 420))
        lift_ride = {'TXID': str(uuid.uuid4()),
                'RFID': rfid,
                'RESORT': resort,
                'LIFT': lift,
                'RIDE_TIME': rtime.isoformat(),
        }
        cur.execute("INSERT INTO lift_ride (data) VALUES (?)", (json.dumps(lift_ride),))
        con.commit
