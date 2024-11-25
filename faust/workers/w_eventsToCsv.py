import faust
import pandas as pd

app = faust.App('EventsToCsv', broker='kafka://localhost:29092')

FilterEvents = app.topic('PercAcc', value_type=bytes)

@app.agent(FilterEvents)
async def getData(records):
    full_df = pd.DataFrame()
    async for i, rec in records.enumerate():
        df = pd.DataFrame.from_dict(rec)
        full_df = pd.concat([full_df, df], ignore_index=True)
        if i == 1200:
            full_df.to_csv('events.csv')