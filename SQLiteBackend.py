import sqlite3

from threading import Lock

lock = Lock()


class SQLiteBackend(object):

    def __init__(self):
        self.con = sqlite3.connect("/app/data/data.db")
        cur = self.con.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS SEASON_PASS (ID INTEGER PRIMARY KEY AUTOINCREMENT, DATA TEXT)"
        )
        self.con.commit()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS RESORT_TICKET (ID INTEGER PRIMARY KEY AUTOINCREMENT, DATA TEXT)"
        )
        self.con.commit()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS LIFT_RIDE (ID INTEGER PRIMARY KEY AUTOINCREMENT, DATA TEXT)"
        )
        self.con.commit()

    def StoreSeasonPass(self, season_pass):
        data = season_pass.toJSON()
        with lock:
            cur = self.con.cursor()
            cur.execute("INSERT INTO SEASON_PASS (DATA) VALUES (?)", (data,))
            self.con.commit()

    def GetSeasonPassBatch(self, after_id, batch_size):
        with lock:
            cur = self.con.cursor()
            cur.execute(
                "SELECT ID, DATA FROM SEASON_PASS WHERE ID > ? LIMIT ?",
                (after_id, batch_size),
            )
            return cur.fetchall()

    def DeleteSeasonPasses(self, before_id):
        with lock:
            cur = self.con.cursor()
            cur.execute("DELETE FROM SEASON_PASS WHERE ID <= ?", (before_id,))
            self.con.commit()

    def StoreResortTicket(self, resort_ticket):
        data = resort_ticket.toJSON()
        with lock:
            cur = self.con.cursor()
            cur.execute("INSERT INTO RESORT_TICKET (DATA) VALUES (?)", (data,))
            self.con.commit()

    def GetResortTicketBatch(self, after_id, batch_size):
        with lock:
            cur = self.con.cursor()
            cur.execute(
                "SELECT ID, DATA FROM RESORT_TICKET WHERE ID > ? LIMIT ?",
                (after_id, batch_size),
            )
            return cur.fetchall()

    def DeleteResortTickets(self, before_id):
        with lock:
            cur = self.con.cursor()
            cur.execute("DELETE FROM RESORT_TICKET WHERE ID <= ?", (before_id,))
            self.con.commit()

    def StoreLiftRide(self, lift_ride):
        data = lift_ride.toJSON()
        with lock:
            cur = self.con.cursor()
            cur.execute("INSERT INTO LIFT_RIDE (DATA) VALUES (?)", (data,))
            self.con.commit()

    def GetLiftRideBatch(self, after_id, batch_size):
        with lock:
            cur = self.con.cursor()
            cur.execute(
                "SELECT ID, DATA FROM LIFT_RIDE WHERE ID > ? LIMIT ?",
                (after_id, batch_size),
            )
            return cur.fetchall()

    def DeleteLiftRides(self, before_id):
        with lock:
            cur = self.con.cursor()
            cur.execute("DELETE FROM LIFT_RIDE WHERE ID <= ?", (before_id,))
            self.con.commit()
