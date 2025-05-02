import json
import uuid
import random

from consts import RESORT_LIFTS


class LiftRide:
    def __init__(self, rfid, resort, rtime):
        self.txid = str(uuid.uuid4())
        self.rfid = rfid
        self.resort = resort
        self.ride_time = rtime.isoformat()
        self.lift = random.choice(RESORT_LIFTS[resort])

    def __str__(self):
        return f"{self.txid}({self.name})"

    def toJSON(self):
        return json.dumps(
            {
                "TXID": self.txid,
                "RFID": self.rfid,
                "RIDE_TIME": self.ride_time,
                "LIFT": self.lift,
                "RESORT": self.resort,
            }
        )
