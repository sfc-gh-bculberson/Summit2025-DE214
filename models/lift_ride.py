"""
Lift ride model for ski resort data.
"""
import hashlib
import random
import json
from dataclasses import dataclass

from consts import RESORT_LIFTS

@dataclass
class LiftRide:
    """Lift ride with realistic properties"""
    txid: str
    rfid: str
    resort: str
    ride_time: str
    lift: str

    @classmethod
    def generate(cls, rfid, resort, rtime, rider_skill=0.5, counter=0):
        """Generate a lift ride with realistic lift selection based on rider skill
        
        Args:
            rfid: RFID of the ticket or pass
            resort: Resort name
            rtime: Ride time
            rider_skill: Rider skill level (0.0 to 1.0, higher is more skilled)
            counter: Counter for deterministic ID generation
            
        Returns:
            LiftRide instance
        """
        # Generate deterministic ID
        txid_seed = f"{rfid}-{resort}-{rtime.isoformat()}-{counter}"
        txid_hash = hashlib.md5(txid_seed.encode()).hexdigest()
        txid = f"RIDE-{txid_hash[:8]}-{txid_hash[8:16]}"

        # Get available lifts for the resort
        lifts = RESORT_LIFTS[resort]

        # Select lift based on rider skill level
        # Beginners (low skill) tend to use lifts at the beginning of the list
        # Experts (high skill) tend to use lifts at the end of the list
        if rider_skill < 0.3:
            # Beginner - focus on first third of lifts
            max_index = max(1, int(len(lifts) / 3))
            lift_index = random.randint(0, max_index)
        elif rider_skill < 0.7:
            # Intermediate - middle lifts
            start_index = int(len(lifts) / 4)
            end_index = int(3 * len(lifts) / 4)
            lift_index = random.randint(start_index, end_index)
        else:
            # Expert - later lifts
            start_index = int(2 * len(lifts) / 3)
            lift_index = random.randint(start_index, len(lifts) - 1)

        # Ensure index is within range
        lift_index = min(lift_index, len(lifts) - 1)
        lift = lifts[lift_index]

        return cls(
            txid=txid,
            rfid=rfid,
            resort=resort,
            ride_time=rtime.isoformat(),
            lift=lift
        )

    def to_json(self):
        """Convert to JSON compatible dictionary"""
        return json.dumps({
            "TXID": self.txid,
            "RFID": self.rfid,
            "RIDE_TIME": self.ride_time,
            "LIFT": self.lift,
            "RESORT": self.resort,
        })