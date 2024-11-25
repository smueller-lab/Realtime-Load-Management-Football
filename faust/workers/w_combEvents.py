import faust
import functions as f
from datetime import datetime


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


EventData = app.topic('EventData', value_type=EventDataRec)
CombEvents = app.topic('CombEvents', value_type=EventDataRec)


# Adding Events together
# Remove events with a length of 0s
prev_event = {}
current_event = {}
event_stats = ['ts_start', 'ts_end', 'v_init', 'max_acc', 'max_speed', 'ongoing']
event_stats_kafka = event_stats[:-1]
@app.agent(EventData, concurrency=22)
async def proc_events(records):
    async for rec in records:
        if rec.duration > 0:
            if rec.player_id in prev_event:
                # calc time between two acceleration events
                time_btw_events = f.get_timediff_event(prev_event[rec.player_id]['ts_end'], rec.ts_start)

                # combining multiple events as long as the time difference is below 0.5 seconds
                if time_btw_events < 0.5:
                    # at the first event which is inside 0.5, save the prev event data is the current
                    # important to save the right start timestamp and start speed
                    if current_event[rec.player_id]['ongoing'] == False:
                        current_event[rec.player_id].update(**prev_event[rec.player_id])

                    # get max speed and acceleration
                    maxacc = max([rec.max_acc, current_event[rec.player_id]['max_acc']])
                    maxspeed = max([rec.max_speed, current_event[rec.player_id]['max_speed']])
                    current_event[rec.player_id].update({'ongoing': True, 'max_acc': maxacc, 'max_speed': maxspeed, 'ts_end': rec.ts_end})

                # either the combined event is finished or there are just two seperate events
                else:
                    # if no combination of events is there send previous event without changes back to Kafka
                    if current_event[rec.player_id]['ongoing'] == False:
                        time = f.get_timediff_event(prev_event[rec.player_id]['ts_start'], prev_event[rec.player_id]['ts_end'])
                        raw_event = EventDataRec(player_id=rec.player_id, team=rec.team, jersey_number=rec.jersey_number,
                                                 duration=time, **prev_event[rec.player_id])
                        await CombEvents.send(partition=rec.player_id, key=str(rec.player_id), value=raw_event)

                    # "ongoing" True means there are atleast two events who need to be combined
                    else:
                        dur = f.get_timediff_event(current_event[rec.player_id]['ts_start'], current_event[rec.player_id]['ts_end'])
                        # exclude attr 'ongoing' from writing into kafka
                        kafka_dict = {key: current_event[rec.player_id][key] for key in event_stats_kafka}

                        comb_e = EventDataRec(player_id=rec.player_id, team=rec.team, jersey_number=rec.jersey_number,
                                              duration=dur, **kafka_dict)
                        await CombEvents.send(partition=rec.player_id, key=str(rec.player_id), value=comb_e)
                        # setting ongoing back to False for next events
                        current_event[rec.player_id]['ongoing'] = False

                # always update the prev_event dict for next comparison
                prev_event[rec.player_id] = {'ts_start': rec.ts_start, 'ts_end': rec.ts_end, 'v_init': rec.v_init,
                                             'max_acc': rec.max_acc, 'max_speed': rec.max_speed}

            else:
                # get current event data, init current event and set "ongoing" to False
                prev_event.update({rec.player_id: {'ts_start': rec.ts_start, 'ts_end': rec.ts_end, 'v_init': rec.v_init,
                                                   'max_acc': rec.max_acc, 'max_speed': rec.max_speed}})
                current_event.update({rec.player_id: dict.fromkeys(event_stats)})
                current_event[rec.player_id]['ongoing'] = False