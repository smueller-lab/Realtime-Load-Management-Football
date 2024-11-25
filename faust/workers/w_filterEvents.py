import faust
from datetime import datetime

# only consider events where the player had a minimum speed and minimum speed increase
# values are coming from the paper
MIN_MAX_SPEED_ACTION = 1.2          # m/s
MIN_SPEED_INCREASE_ACTION = 0.9     # m/s

app = faust.App('master', broker='kafka://localhost:29092')

class EventDataRec(faust.Record, validation=True, serializer='json'):
    player_id: int
    team: int
    jersey_number: int
    ts_start: str
    ts_end: str
    duration: float
    v_init: float
    max_acc: float
    max_speed: float

CombEvents = app.topic('CombEvents', value_type=EventDataRec)
FilterEvents = app.topic('FilterEvents', value_type=EventDataRec)

# Filter events by:
# - Minimum speed increase of 0.9 m/s (paper)
# - Minimum max speed of 1.2 m/s (paper)
@app.agent(CombEvents, concurrency=22)
async def filter_events(records):
    async for rec in records:
        if rec.max_speed > MIN_MAX_SPEED_ACTION:
            speed_inc = rec.max_speed - rec.v_init
            if speed_inc > MIN_SPEED_INCREASE_ACTION:
                await FilterEvents.send(partition=rec.player_id, key=str(rec.player_id), value=rec)