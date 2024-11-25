import faust
import numpy as np
import functions as f
from datetime import datetime

# data from paper about percentage accleration
V_INIT = np.array([0, 6.2, 11.4, 16.7])
MAX_ACC = np.array([6.01, 4.33, 3.20, 2.29])

# fit least square method and return slope and intercept of the fitted line
def get_slope_intercept(xs, ys):
    sum_xsquare = sum([x * x for x in xs])
    sum_xy = sum(xs * ys)
    n = len(xs)
    slope = (n * sum_xy - sum(xs) * sum(ys)) / ((n * sum_xsquare) - (sum(xs) * sum(xs)))
    intercept = (sum(ys) - (slope * sum(xs))) / n
    return slope, intercept

# get maximal volutary acceleration from initial speed
def get_voluntary_maxAcc(v_init):
    m, b = get_slope_intercept(V_INIT, MAX_ACC)
    amax_vol = (m*v_init)+b
    return amax_vol

# calculate percentage accerleration
def get_percAcc(amax_action, v_init):
    amax_vol = get_voluntary_maxAcc(v_init)
    return (amax_action/amax_vol) * 100

app = faust.App('master', broker='kafka://localhost:29092')

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

class percAccEventRec(faust.Record, validation=True, serializer='json'):
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
    percAcc: float


ClassEvents = app.topic('ClassEvents', value_type=classEventsRec)
percAccEvent = app.topic('PercAcc', value_type=percAccEventRec)

@app.agent(ClassEvents, concurrency=22)
async def calcPercAcc(records):
    async for rec in records:
        data_dict = f.faustRecord_toDict(rec)
        v_init_kmh = rec.v_init * 3.6
        percentageAcc = get_percAcc(amax_action=rec.max_acc,v_init=v_init_kmh)
        finaldata = percAccEventRec(percAcc=percentageAcc, **data_dict)
        await percAccEvent.send(value=finaldata, key=str(rec.player_id), partition=rec.player_id)