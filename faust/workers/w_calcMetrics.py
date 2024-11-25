import faust
from datetime import datetime

# time between two messages of one player
TIME_DIFF = 0.04
# number of points to calculate small sliding average for actual acceleration
SMALL_ACC = 8
# number of points to calculate bigger sliding average for event detection
BIG_ACC = 16

app = faust.App('master', broker='kafka://localhost:29092')


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
    x: float
    y: float
    speed: float
    player_id: int
    acc_actual: float
    acc_event: float


# topic definition
ingestion = app.topic('ingestion', value_type=ingestionRec)
TrackingAcc = app.topic('Tracking', value_type=TrackingAccRec)

# calculate acceleration of each player
prev_speed = {}
acc_list = {}


@app.agent(ingestion, concurrency=22)
async def calcAcc(records):
    async for rec in records:
        # check if player has already data in prev_speed
        # calculate acceleration and save current speed in dict
        if rec.player_id in prev_speed:
            # acc = (rec.speed - prev_speed[rec.player_id]) / TIME_DIFF
            if len(prev_speed[rec.player_id]) >= SMALL_ACC:
                avg_acc_small = (rec.speed - prev_speed[rec.player_id][-SMALL_ACC]) / (SMALL_ACC * TIME_DIFF)
                # prev_speed[rec.player_id].pop(0)

                if len(prev_speed[rec.player_id]) >= BIG_ACC:
                    avg_acc_big = (rec.speed - prev_speed[rec.player_id][-BIG_ACC]) / (BIG_ACC * TIME_DIFF)
                    prev_speed[rec.player_id].pop(0)

                # if acc_big already exist write in into kafka, else write 0 for acc_big
                try:
                    accData = TrackingAccRec(timestamp=rec.timestamp, team=rec.team, jersey_number=rec.jersey_number,
                                             x=float(rec.x / 100), y=float(rec.y / 100), speed=rec.speed,
                                             player_id=rec.player_id,
                                             acc_actual=avg_acc_small, acc_event=avg_acc_big)
                except NameError:
                    accData = TrackingAccRec(timestamp=rec.timestamp, team=rec.team, jersey_number=rec.jersey_number,
                                             x=float(rec.x / 100),
                                             y=float(rec.y / 100), speed=rec.speed, player_id=rec.player_id,
                                             acc_actual=avg_acc_small, acc_event=0)

                # send data to Kafka
                await TrackingAcc.send(partition=rec.player_id, key=str(rec.player_id), value=accData)

                prev_speed[rec.player_id].append(rec.speed)

            else:
                prev_speed[rec.player_id].append(rec.speed)

        # difference of speed is needed for acceleration
        else:
            prev_speed.update({rec.player_id: [rec.speed]})