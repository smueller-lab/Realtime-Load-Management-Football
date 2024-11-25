import faust
import functions as f

# Get the speedzone of a current speed of a player
def get_speedZone(speed):
    # speed from m/s to km/h
    speed *= 3.6
    speed_zones = {'1': [0, 11], '2': [11.001, 14], '3': [14.001, 19], '4': [19.001, 23], '5': [23.001, 100]}
    for item in speed_zones:
        if speed >= speed_zones[item][0] and speed <= speed_zones[item][1]:
            return item


app = faust.App('sz', broker='kafka://localhost:29092')

class covDistancesRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    x: float
    y: float
    speed: float
    player_id: int
    dist: float

class speedZonesRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    x: float
    y: float
    speed: float
    player_id: int
    dist: float
    speedZone: int


covDistances = app.topic('covDistances', value_type=covDistancesRec)
speedZones = app.topic('SpeedZones', value_type=speedZonesRec)


@app.agent(covDistances)
async def get_speed_zone(records):
    async for rec in records:
        speed_zone = get_speedZone(rec.speed)
        data_dict = f.faustRecord_toDict(rec)
        await speedZones.send(key=str(rec.player_id), value=speedZonesRec(speedZone=speed_zone, **data_dict), partition=rec.player_id)
