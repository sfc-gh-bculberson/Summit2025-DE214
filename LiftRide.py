import json
import uuid
import random

from consts import RESORT_LIFTS


class LiftRide:
    def __init__(self, rfid, resort, rtime):
        self.TXID = str(uuid.uuid4())
        self.RFID = rfid
        self.RESORT = resort
        self.RIDE_TIME = rtime.isoformat()
        self.LIFT = random.choice(RESORT_LIFTS[resort])

    def __str__(self):
        return f"{self.txid}({self.name})"

    def toJSON(self):
        return json.dumps(
            {
                "TXID": self.TXID,
                "RFID": self.RFID,
                "RIDE_TIME": self.RIDE_TIME,
                "LIFT": self.LIFT,
                "RESORT": self.RESORT,
            }
        )
