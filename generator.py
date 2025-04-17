import os
import datetime
import random

from SQLiteBackend import SQLiteBackend
from consts import *
from SeasonPass import SeasonPass
from ResortTicket import ResortTicket
from LiftRide import LiftRide

backend = SQLiteBackend()
tickets_per_event_loop = 5
tickets_to_season_pass_ratio = 20


def next(world_time):
    speed = os.getenv("SPEED")
    diff = datetime.datetime.now(datetime.UTC) - world_time
    # SIM CITY SPEEDS
    # TURTLE: 1 day == 12 min, LLAMA: 1 day == 3 min, CHEETAH: 1 day == 90 sec
    if speed == "TURTLE":
        return world_time + datetime.timedelta(seconds=diff.seconds * 120)
    elif speed == "LLAMA":
        return world_time + datetime.timedelta(seconds=diff.seconds * 480)
    else:
        return world_time + datetime.timedelta(seconds=diff.seconds * 960)


def event_loop():
    world_time = datetime.datetime.now(datetime.UTC)
    tickets_purchased = 0
    season_passes_purchased = 0
    season_passes = []
    resort_tickets = []
    while True:
        # Add Season Passes, sold 24x7
        season_passes_needed = int(tickets_purchased / tickets_to_season_pass_ratio)
        while season_passes_needed > season_passes_purchased:
            season_pass = SeasonPass(world_time)
            season_passes.append(season_pass)
            backend.StoreSeasonPass(season_pass)
            season_passes_purchased += 1
        # Add Resort Tickets, sold 24x7
        resorts = random.choices(
            RESORTS, weights=RESORT_WEIGHTS, k=tickets_per_event_loop
        )
        for resort in resorts:
            resort_ticket = ResortTicket(resort, world_time)
            resort_tickets.append(resort_ticket)
            backend.StoreResortTicket(resort_ticket)
            tickets_purchased += 1
        # Add Lift Rides, 8:30->16:00
        for resort_ticket in resort_tickets:
            riding, resort = resort_ticket.isRidingToday(world_time)
            if riding:
                open_time = world_time.astimezone(
                    RESORT_TZS[RESORTS.index(resort)]
                ).replace(hour=8, minute=30)
                close_time = world_time.astimezone(
                    RESORT_TZS[RESORTS.index(resort)]
                ).replace(hour=16)
            if (
                riding
                and world_time.astimezone(RESORT_TZS[RESORTS.index(resort)])
                >= open_time
                and world_time.astimezone(RESORT_TZS[RESORTS.index(resort)])
                < close_time
            ):
                if resort_ticket.needsRide(world_time):
                    lift_ride = LiftRide(resort_ticket.rfid, resort, world_time)
                    backend.StoreLiftRide(lift_ride)
        resort_tickets = [t for t in resort_tickets if not t.isExpired(world_time)]
        # Add Season Ticket Holder Rides, 8:30->16:00
        for season_pass in season_passes:
            riding, resort = resort_ticket.isRidingToday(world_time)
            if riding:
                open_time = world_time.astimezone(
                    RESORT_TZS[RESORTS.index(resort)]
                ).replace(hour=8, minute=30)
                close_time = world_time.astimezone(
                    RESORT_TZS[RESORTS.index(resort)]
                ).replace(hour=16)
            if (
                riding
                and world_time.astimezone(RESORT_TZS[RESORTS.index(resort)])
                >= open_time
                and world_time.astimezone(RESORT_TZS[RESORTS.index(resort)])
                < close_time
            ):
                if riding and resort_ticket.needsRide(world_time):
                    lift_ride = LiftRide(resort_ticket.rfid, resort, world_time)
                    backend.StoreLiftRide(lift_ride)
        season_passes = [sp for sp in season_passes if not sp.isExpired(world_time)]
        world_time = next(world_time)


if __name__ == "__main__":
    event_loop()
