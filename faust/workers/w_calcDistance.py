import faust
import numpy as np


app = faust.App('sz', broker='kafka://localhost:29092')

class ingestionRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    x: float
    y: float
    speed: float
    half: int
    teamjersey: int
    player_id: int

class covDistancesRec(faust.Record, validation=True, serializer='json'):
    timestamp: str
    team: int
    jersey_number: int
    x: float
    y: float
    speed: float
    player_id: int
    dist: float


ingestion = app.topic('ingestion', value_type=ingestionRec)
covDistances = app.topic('covDistances', value_type=covDistancesRec)

player_pos = {}
@app.agent(ingestion)
async def getDistance(records):
    async for rec in records:
        x_val = rec.x/100
        y_val = rec.y/100
        if rec.player_id in player_pos:
            dist_cov = np.sqrt((player_pos[rec.player_id][0] - x_val)**2 + (player_pos[rec.player_id][1] - y_val)**2)
            player_pos.update({rec.player_id: [x_val, y_val]})
            distVal = covDistancesRec(timestamp=rec.timestamp, team=rec.team, jersey_number=rec.jersey_number,
                                      x=x_val, y=y_val, speed=rec.speed, player_id=rec.player_id, dist=dist_cov)
            await covDistances.send(value=distVal, key=str(rec.player_id), partition=rec.player_id)

        else:
            player_pos.update({rec.player_id: [x_val, y_val]})