import faust
import pandas as pd
import functions as f
from datetime import datetime

HIGH_VINIT = 4
LONG_RUN = 4

hist_data = pd.read_csv('../data.csv')
ACC_MEDIAN = hist_data['max_acc'].describe()['50%']
SPEED_MEDIAN = hist_data['max_speed'].describe()['50%']

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

class classEventsRec(faust.Record, validation=True, serializer='json'):
    player_id: int
    team: int
    jersey_number: int
    ts_start: str
    ts_end: str
    duration: float
    v_init: float
    max_acc: float
    max_speed: float
    event_class: str

FilterEvents = app.topic('FilterEvents', value_type=EventDataRec)
ClassEvents = app.topic('ClassEvents', value_type=classEventsRec)

# get class of Event
def getClassEvent(max_acc, max_speed, v_init, duration):
    class_event = ""
    # low acc, low speed --> increasing run
    if max_acc <= ACC_MEDIAN and max_speed <= SPEED_MEDIAN:
        if duration > LONG_RUN:
            class_event = "Long increasing run"
        else:
            class_event = "Short increasing run"
    # low acc, high speed --> Intensity run depending on vinit
    if max_acc <= ACC_MEDIAN and max_speed > SPEED_MEDIAN:
        if v_init > HIGH_VINIT:
            class_event = "High intensity run"
        else:
            class_event = "Low intensity run"
    # high acc, low speed --> feint
    if max_acc > ACC_MEDIAN and max_speed <= SPEED_MEDIAN:
        class_event = "Feint"
    # high acc, high speed
    if max_acc > ACC_MEDIAN and max_speed > SPEED_MEDIAN:
        if v_init > HIGH_VINIT:
            class_event = "High intensity sprint"
        else:
            class_event = "Low intensity sprint"

    return class_event


@app.agent(FilterEvents, concurrency=22)
async def get_classEvents(records):
    async for rec in records:
        eclass = getClassEvent(rec.max_acc, rec.max_speed, rec.v_init, rec.duration)
        data_dict = f.faustRecord_toDict(rec)
        classEvent = classEventsRec(event_class=eclass, **data_dict)
        await ClassEvents.send(partition=rec.player_id, key=str(rec.player_id), value=classEvent)