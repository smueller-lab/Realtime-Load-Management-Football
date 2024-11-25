import faust
import pandas as pd

app = faust.App('EventsToCsv', broker='kafka://localhost:29092')

Speed = app.topic('SpeedZones', value_type=bytes)

@app.agent(Speed)
async def getData(records):
    full_df = pd.DataFrame()
    async for i, rec in records.enumerate():
        df = pd.DataFrame.from_dict(rec)
        full_df = pd.concat([full_df, df], ignore_index=True)
        if i == 10:
            full_df.to_csv('protocol_events.csv')