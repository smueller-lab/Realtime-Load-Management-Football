import faust
import aredis
import json
import credentials as cred

app = faust.App('master', broker='kafka://localhost:29092')

PercAcc = app.topic('PercAcc', value_type=bytes)

# connection to Redis db ('aredis' is package of 'redis' for async functions)
client = aredis.StrictRedis(host=cred.REDIS_HOST, port=cred.REDIS_PORT, db=1, password=cred.REDIS_PW)

# sending data to Redis database as Key-value pairs
@app.agent(PercAcc)
async def get_data(records):
    async for i, rec in records.enumerate():
        await client.set(i, json.dumps(rec))