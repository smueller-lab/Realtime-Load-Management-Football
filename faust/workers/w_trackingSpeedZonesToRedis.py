import faust
import aredis
import json
import credentials as cred

app = faust.App('sz', broker='kafka://localhost:29092')

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

speedZones = app.topic('SpeedZones', value_type=bytes)

# connection to Redis db ('aredis' is package of 'redis' for async functions)
client = aredis.StrictRedis(host=cred.REDIS_HOST, port=cred.REDIS_PORT, db=2, password=cred.REDIS_PW)

# sending data to Redis database as Key-value pairs
@app.agent(speedZones)
async def get_data(records):
    records_list = []
    async for i, rec in records.enumerate():
        records_list.append(json.dumps(rec))
        # send tracking data to redis with an expiration of 120 seconds
        if len(records_list) > 100:
            await client.set(i, records_list)
            await client.expire(i, 120)
            records_list = []