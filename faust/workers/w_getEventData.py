import faust
import functions as f
import credentials as cred
from datetime import datetime
import aredis

MIN_SPEED = 4

app = faust.App('master', broker='kafka://localhost:29092')


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


TrackingStartEvents = app.topic('TrackingStartEvents', value_type=TrackingStartEventsRec)
EventData = app.topic('EventData', value_type=EventDataRec)

client = aredis.StrictRedis(host=cred.REDIS_HOST, port=cred.REDIS_PORT, db=3, password=cred.REDIS_PW)

# extract data of an acceleration event
event = {}
actual_ts = {}
dist = {}
@app.agent(TrackingStartEvents)
async def get_eventdata(records):
    async for rec in records:
        if rec.player_id in dist:
            dist[rec.player_id]['dist'][str(rec.speedzone)] += rec.dist
            await client.set(rec.player_id, dist[rec.player_id])

        else:
            dist.update({rec.player_id: {'team': rec.team, 'player': rec.jersey_number,
                                         'dist': {'1': 0, '2': 0, '3': 0, '4': 0, '5': 0}}})
            dist[rec.player_id]['dist'][str(rec.speedzone)] += rec.dist

        # check for event start, yes: save start time and start speed
        if rec.player_id not in event:
            actual_ts.update({rec.player_id: {'ts': rec.timestamp}})
            if rec.start_event == 1:
                event.update(
                    {rec.player_id: {'ts_start': rec.timestamp, 'v_init': rec.speed, 'max_acc': 0, 'max_speed': 0}})

        # either an event is ongoing or no event at all is happening
        else:
            # check if an event has started for a player, else do nothing
            if rec.player_id in event:
                passed_time = f.get_timediff_event(actual_ts[rec.player_id]['ts'], rec.timestamp)
                # check if event acc is greater than 0 and passed time between two measuring
                # points is less than 1 second --> event is ongoing
                # if it's bigger then there is the halftime and event should be ended with the last saved timestamp
                if (rec.acc_event >= 0 or rec.speed >= MIN_SPEED) and passed_time < 1:
                    # keep track of the highest acc in the event
                    if rec.acc_actual > event[rec.player_id]['max_acc']:
                        event[rec.player_id].update({'max_acc': rec.acc_actual})
                    if rec.speed > event[rec.player_id]['max_speed']:
                        event[rec.player_id].update({'max_speed': rec.speed})
                    # save potential current end timestamp
                    event[rec.player_id].update({'ts_end': rec.timestamp})

                # if actual speed is NOT higher than the last one, the event is finished
                # calculate duration of event and send data to Kafka
                else:
                    # checks if an event started, only events are sent back to Kafka --> not all tracking data
                    if 'ts_end' in event[rec.player_id]:
                        time_diff = f.get_timediff_event(event[rec.player_id]['ts_start'],
                                                         event[rec.player_id]['ts_end'])
                        event_data = EventDataRec(player_id=rec.player_id, team=rec.team,
                                                  jersey_number=rec.jersey_number,
                                                  duration=time_diff, **event[rec.player_id])
                        await EventData.send(partition=rec.player_id,
                                             key=str(rec.player_id),
                                             value=event_data)
                        del event[rec.player_id]

            actual_ts.update({rec.player_id: {'ts': rec.timestamp}})