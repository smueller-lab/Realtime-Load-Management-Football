import faust
import functions as f
import credentials as cred
from datetime import datetime
import numpy as np
import aredis
import json


# Get the speedzone of a current speed of a player
def get_speedZone(speed):
    # speed from m/s to km/h
    speed *= 3.6
    speed_zones = {'1': [0, 11], '2': [11.001, 14], '3': [14.001, 19], '4': [19.001, 23], '5': [23.001, 100]}
    for item in speed_zones:
        if speed >= speed_zones[item][0] and speed <= speed_zones[item][1]:
            return item


app = faust.App('master', broker='kafka://localhost:29092')


class TrackingAccRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    x: float
    y: float
    speed: float
    player_id: int
    acc_actual: float
    acc_event: float


class TrackingStartEventsRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    x: float
    y: float
    speed: float
    speedzone: int
    dist: float
    player_id: int
    acc_actual: float
    acc_event: float
    start_event: int


TrackingAcc = app.topic('Tracking', value_type=TrackingAccRec)
TrackingStartEvents = app.topic('TrackingStartEvents', value_type=TrackingStartEventsRec)

client = aredis.StrictRedis(host=cred.REDIS_HOST, port=cred.REDIS_PORT, db=2, password=cred.REDIS_PW)

# detecting the start of an acceleration event
prev_vals = {}


@app.agent(TrackingAcc)
async def get_eventstarts(records):
    records_list = []
    async for i, rec in records.enumerate():
        # get event starts
        x_val = rec.x
        y_val = rec.y
        if rec.player_id in prev_vals:
            # acc shift from negative to positive indicates start of an event
            if prev_vals[rec.player_id]['acc'] <= 0 and rec.acc_event > 0:
                event_start = 1
            else:
                event_start = 0

            # calculate distance and speedzone
            dist_cov = np.sqrt(
                (prev_vals[rec.player_id]['x'] - x_val) ** 2 + (prev_vals[rec.player_id]['y'] - y_val) ** 2)
            speed_zone = get_speedZone(rec.speed)
            prev_vals.update({rec.player_id: {'acc': rec.acc_event, 'x': x_val, 'y': y_val}})

            data_dict = f.faustRecord_toDict(rec)
            new_rec = TrackingStartEventsRec(start_event=event_start, dist=dist_cov, speedzone=speed_zone, **data_dict)

            raw_data = new_rec.dumps().decode('UTF-8')
            records_list.append(raw_data)
            if len(records_list) > 22:
                await client.set(i, records_list)
                await client.expire(i, 120)
                records_list = []

            await TrackingStartEvents.send(partition=rec.player_id, key=str(rec.player_id), value=new_rec)

        else:
            prev_vals.update({rec.player_id: {'acc': rec.acc_event, 'x': x_val, 'y': y_val}})