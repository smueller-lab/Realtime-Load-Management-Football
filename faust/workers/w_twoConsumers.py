import faust
import numpy as np

# time between two messages of one player
TIME_DIFF = 0.4
# number of points to calculate small sliding average for actual acceleration
SMALL_ACC = 3
# number of points to calculate bigger sliding average for event detection
BIG_ACC = 10

app = faust.App('events', broker='kafka://localhost:29092')

# raw data Schema
class ingestionRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    x: float
    y: float
    speed: float
    half: int
    teamjersey: int
    player_id: int

class TrackingAccRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    speed: float
    player_id: int
    acc_actual: float
    acc_event: float

class covDistancesRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    x: float
    y: float
    speed: float
    player_id: int
    dist: float


# topic definition
ingestion = app.topic('ingestion', value_type=ingestionRec)
TrackingAcc = app.topic('TrackingAcc', value_type=TrackingAccRec)
covDistances = app.topic('covDistances', value_type=covDistancesRec)


# calculate acceleration of each player
prev_speed = {}
acc_list = {}
@app.agent(ingestion)
async def calcAcc(records):
    async for rec in records:
        # check if player has already data in prev_speed
        # calculate acceleration and save current speed in dict
        if rec.player_id in prev_speed:
            acc = (rec.speed - prev_speed[rec.player_id]) / TIME_DIFF
            prev_speed[rec.player_id] = rec.speed

            # calc average acc and write data into new kafka topic
            if rec.player_id in acc_list:
                # calculate sliding average of actual acceleration
                if len(acc_list[rec.player_id]) >= SMALL_ACC:
                    avg_acc_small = sum(acc_list[rec.player_id][-SMALL_ACC:]) / SMALL_ACC

                    # if enough points, calculate acceleration for event detection
                    # delete first entry to make space for the new one later on
                    if len(acc_list[rec.player_id]) == BIG_ACC:
                        avg_acc_big = sum(acc_list[rec.player_id]) / BIG_ACC
                        del acc_list[rec.player_id][0]

                    # if acc_big already exist write in into kafka, else write 0 for acc_big
                    try:
                        accData = TrackingAccRec(timestamp=rec.timestamp, team=rec.team, jersey_number=rec.jersey_number,
                                                 speed=rec.speed, player_id=rec.player_id,
                                                 acc_actual=avg_acc_small, acc_event=avg_acc_big)
                    except NameError:
                        accData = TrackingAccRec(timestamp=rec.timestamp, team=rec.team, jersey_number=rec.jersey_number,
                                                 speed=rec.speed, player_id=rec.player_id, acc_actual=avg_acc_small, acc_event=0)

                    # send data to Kafka
                    await TrackingAcc.send(partition=rec.player_id, key=str(rec.player_id), value=accData)

                acc_list[rec.player_id].append(acc)

            else:
                acc_list.update({rec.player_id: [acc]})

        # difference of speed is needed for acceleration
        else:
            prev_speed.update({rec.player_id: rec.speed})


player_pos = {}
@app.agent(ingestion)
async def getDistance(records):
    async for rec in records:
        x_val = rec.x/100
        y_val = rec.y/100
        if rec.player_id in player_pos:
            dist_cov = np.sqrt((player_pos[rec.player_id][0] - x_val)**2 + (player_pos[rec.player_id][1] - y_val)**2)
            player_pos.update({rec.player_id: [x_val, y_val]})
            distVal = covDistancesRec(timestamp=rec.timestamp, team=rec.team, jersey_number=rec.jersey_number,
                                      x=x_val, y=y_val, speed=rec.speed, player_id=rec.player_id, dist=dist_cov)
            await covDistances.send(value=distVal, key=str(rec.player_id), partition=rec.player_id)

        else:
            player_pos.update({rec.player_id: [x_val, y_val]})