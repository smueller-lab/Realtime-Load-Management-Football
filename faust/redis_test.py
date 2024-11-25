import redis
import json
import pandas as pd


# credentials for Redis database
DOCKER_IP = '86.119.35.243'
REDIS_PW = 'abc123!'
REDIS_PORT = 6379


def get_events_from_redisdb(db_number):
    client = redis.StrictRedis(host=DOCKER_IP, port=REDIS_PORT, db=db_number, password=REDIS_PW, charset="utf-8", decode_responses=True)
    raw_data = client.mget(client.keys())
    data_dict = [json.loads(idx.replace("'", '"')) for idx in raw_data]
    df = pd.DataFrame(data_dict)
    df = df.iloc[:, :-1]
    return df
    
def get_speedzones_from_redisdb(db_number):
    client = redis.StrictRedis(host=DOCKER_IP, port=REDIS_PORT, db=db_number, password=REDIS_PW, charset="utf-8", decode_responses=True)
    raw_data = client.mget(client.keys())
    only_dict = [[i for i in raw_data[p].split("'") if len(i) > 10] for p in range(len(raw_data))]
    full_dict = [x for x in only_dict for x in x]
    data_dict = [json.loads(idx.replace("'", '"')) for idx in full_dict]
    df = pd.DataFrame(data_dict)
    df = df.iloc[:, :-1]
    return df
    
def get_sz_from_redisdb(db_number):
    client = redis.StrictRedis(host=DOCKER_IP, port=REDIS_PORT, db=db_number, password=REDIS_PW, charset="utf-8", decode_responses=True)
    raw_data = client.mget(client.keys())
    data_dict = [json.loads(idx.replace("'", '"')) for idx in raw_data]
    df = pd.DataFrame(data_dict)
    return df
    
def get_events_routes_player(events, sz, player):
    events = events[events['player_id'] == player]
    sz = sz[sz['player_id'] == player]

    TIME_FORMAT = '%Y-%m-%d''T''%H:%M:%S.%f%z'
    events.loc[:,'ts_start'] = pd.to_datetime(events['ts_start'], format=TIME_FORMAT)
    events.loc[:,'ts_end'] = pd.to_datetime(events['ts_end'], format=TIME_FORMAT)
    sz.loc[:,'timestamp'] = pd.to_datetime(sz['timestamp'], format=TIME_FORMAT)

    # get x and y values of data of events
    list_df = []
    for index, row in events.iterrows():
        start = row['ts_start']
        end = row['ts_end']
        df = sz[sz['timestamp'].between(start, end)]
        if df.empty:
            pass
        else:
            list_df.append(df)

    final_df = pd.concat(list_df, ignore_index=True)
    # convert x and y values to new coordinate system
    final_df['x'] = final_df['x'] + 52.5
    final_df['y'] = final_df['y'] + 34
    return final_df


events = get_events_from_redisdb(1)
sz = get_speedzones_from_redisdb(2)

print(sz)

data = get_events_routes_player(events=events, sz=sz, player=2)


print(data)