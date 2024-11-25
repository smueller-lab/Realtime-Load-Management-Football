import faust
import aredis
import json
import credentials as cred

app = faust.App('sz2', broker='kafka://localhost:29092')

finalSpeedZones = app.topic('finalSpeedZones', value_type=bytes)

client = aredis.StrictRedis(host=cred.REDIS_HOST, port=cred.REDIS_PORT, db=3, password=cred.REDIS_PW)

# get grouped speedzones from ksqldb and send data to Redis db
@app.agent(finalSpeedZones)
async def trackingData_toRedis(records):
    async for key, value in records.items():
        k = json.loads(key.decode('utf-8'))
        idx = str(k['TEAM']) + str(k['JERSEY_NUMBER']) + str(k['SPEEDZONE'])
        k.update({'total_distance': value['TOTAL_DISTANCE']})
        await client.set(idx, k)