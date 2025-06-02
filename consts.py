from zoneinfo import ZoneInfo

# Resorts configuration
RESORTS = ["Vail", "Beaver Creek", "Breckenridge", "Keystone", "Heavenly"]
RESORT_WEIGHTS = [0.25, 0.2, 0.25, 0.2, 0.1]
RESORT_TZS = [
    ZoneInfo("America/Denver"),
    ZoneInfo("America/Denver"),
    ZoneInfo("America/Denver"),
    ZoneInfo("America/Denver"),
    ZoneInfo("America/Los_Angeles"),
]

# Resort profiles for realistic pricing
RESORT_PROFILES = {
    'Vail': {
        'ticket_base_price': 120,
        'weekend_multiplier': 1.6,
    },
    'Beaver Creek': {
        'ticket_base_price': 110,
        'weekend_multiplier': 1.5,
    },
    'Breckenridge': {
        'ticket_base_price': 100,
        'weekend_multiplier': 1.7,
    },
    'Keystone': {
        'ticket_base_price': 90,
        'weekend_multiplier': 1.6,
    },
    'Heavenly': {
        'ticket_base_price': 110,
        'weekend_multiplier': 1.5,
    }
}

# Ticket duration options and weights
TICKET_DAY_OPTIONS = [1, 2, 3, 4, 5, 6, 7]
TICKET_DAY_WEIGHTS = [0.35, 0.35, 0.1, 0.05, 0.05, 0.05, 0.05]

# Ride timing constants (in minutes)
RIDE_MIN_INTERVAL = 10  # Minimum time between rides
RIDE_MAX_INTERVAL = 30  # Maximum time between rides
REST_MIN_INTERVAL = 30  # Minimum rest time
REST_MAX_INTERVAL = 60  # Maximum rest time

# Riding probability constants
DAILY_TICKET_RIDING_CHANCE = 0.85  # Chance of a daily ticket holder riding on a given day
SEASON_PASS_RIDING_CHANCE = 0.10   # Chance of a season pass holder riding on a given day

# Generator processing limits
MAX_TICKETS_PER_LOOP = 1000        # Maximum tickets to process per loop
MAX_PASSES_PER_LOOP = 200          # Maximum passes to process per loop

# Simulation speed options
SPEED_SETTINGS = {
    "TURTLE": 120,   # 1 day = 12 minutes (120x multiplier)
    "LLAMA": 480,    # 1 day = 3 minutes (480x multiplier)
    "CHEETAH": 960,  # 1 day = 90 seconds (960x multiplier)
}

# Lift configurations by resort
VAIL_LIFTS = [
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
BEAVER_LIFTS = [
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
BRECKENRIDGE_LIFTS = [
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
    "Castle Carpet 1",
]
KEYSTONE_LIFTS = [
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
HEAVENLY_LIFTS = [
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

RESORT_LIFTS = {
    "Vail": VAIL_LIFTS,
    "Beaver Creek": BEAVER_LIFTS,
    "Breckenridge": BRECKENRIDGE_LIFTS,
    "Keystone": KEYSTONE_LIFTS,
    "Heavenly": HEAVENLY_LIFTS,
}