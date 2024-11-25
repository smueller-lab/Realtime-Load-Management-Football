import dash
from dash import html
from dash import dcc
from dash import dash_table
import dash_player
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import redis
import json
import pandas as pd
from datetime import datetime
import credentials as cred

# Time formats
TIME_FORMAT = '%Y-%m-%d''T''%H:%M:%S.%f%z'
START_TIME_VIDEO = '2019-06-05T20:45:00.000+0100'


# get data as pandas df from redis database
def get_df_from_redisdb():
    client = redis.StrictRedis(host=cred.DOCKER_IP, port=cred.REDIS_PORT, db=1, password=cred.REDIS_PW, charset="utf-8",
                               decode_responses=True)
    raw_data = client.mget(client.keys())
    data_dict = [json.loads(idx.replace("'", '"')) for idx in raw_data]
    df = pd.DataFrame(data_dict)
    df = df.iloc[:, :-1]
    return df


def get_speedzones_from_redisdb():
    client = redis.StrictRedis(host=cred.DOCKER_IP, port=cred.REDIS_PORT, db=2, password=cred.REDIS_PW, charset="utf-8",
                               decode_responses=True)
    raw_data = client.mget(client.keys())
    raw_data = list(filter(None, raw_data))
    only_dict = [[i for i in raw_data[p].split("'") if len(i) > 10] for p in range(len(raw_data))]
    full_dict = [x for x in only_dict for x in x]
    data_dict = [json.loads(idx.replace("'", '"')) for idx in full_dict]
    df = pd.DataFrame(data_dict)
    df = df.iloc[:, :-1]
    return df


def get_sumSpeedZones_from_redisdb():
    client = redis.StrictRedis(host=cred.DOCKER_IP, port=cred.REDIS_PORT, db=3, password=cred.REDIS_PW, charset="utf-8",
                               decode_responses=True)
    raw_data = client.mget(client.keys())
    data_dict = [json.loads(idx.replace("'", '"')) for idx in raw_data]
    df = pd.DataFrame(data_dict)
    dist = df['dist'].apply(pd.Series)
    df2 = pd.concat([df[['team', 'player']], dist], axis=1)
    df3 = df2.melt(id_vars=['team', 'player'], var_name='speedzone', value_name='dist')
    return df3


# get time difference in seconds
def get_timediff_event(earlier_event, later_event):
    time_diff = datetime.strptime(later_event, TIME_FORMAT) - datetime.strptime(earlier_event, TIME_FORMAT)
    return time_diff.total_seconds()


# Build App
app = dash.Dash(external_stylesheets=[dbc.themes.SLATE])

# basic layout for Dash plots
layout_dict = {'template': 'plotly_dark', 'plot_bgcolor': 'rgba(0, 0, 0, 0)', 'paper_bgcolor': 'rgba(0, 0, 0, 0)'}


def get_sortedTable_fromDB(df):
    del df['ts_end']
    df['ts_start'] = pd.to_datetime(df['ts_start'])
    # sort values to determine the latest message
    df = df.sort_values(by='ts_start')
    # get the oldest allowed message [this case 2 minutes]
    oldest_ts = df['ts_start'].iloc[-1] - pd.Timedelta(2, 'minutes')
    # filter df with messages in the last two minutes
    twomin = df[df["ts_start"] >= oldest_ts]
    # sort values by perc Acc
    final_table = twomin.sort_values(by='percAcc', ascending=False).head(10)
    percAccTable = final_table.round(2)

    percAccTable['team'] = percAccTable['team'].replace([0, 1], ['CH', 'PT'])
    percAccTable['team-pl'] = percAccTable['team'].astype(str) + '-' + percAccTable['jersey_number'].astype(str)
    percAccTable['ts_start'] = percAccTable['ts_start'].dt.time
    percAccTable[['v_init', 'max_speed']] = percAccTable[['v_init', 'max_speed']] * 3.6
    percAccTable[['v_init', 'max_speed']] = percAccTable[['v_init', 'max_speed']].round(decimals=3)
    percAccTable = percAccTable[
        ['team-pl', 'ts_start', 'duration', 'v_init', 'max_acc', 'max_speed', 'event_class', 'percAcc']]
    percAccTable.columns = ['player', 'event start', 'duration[s]', 'v init[km/h]', 'max acc[m/s²]', 'max speed[km/h]',
                            'event class', 'percAcc']
    return percAccTable


# get percentage of total percAcc from all players
def group_sum_percAcc_percent(df):
    events_perc = df.groupby('player_id', as_index=False).agg(
        {'percAcc': sum, 'team': 'first', 'jersey_number': 'first'})
    events_perc['percAccperc'] = (events_perc['percAcc'] / events_perc['percAcc'].sum()) * 100
    events_perc['team'] = events_perc['team'].replace([0, 1], ['CH', 'PT'])
    events_perc['team-pl'] = events_perc['team'].astype(str) + '-' + events_perc['jersey_number'].astype(str)
    events_perc = events_perc.sort_values(by='percAcc', ascending=False)
    events_perc = events_perc.head(10)
    return events_perc


# sum speed zones
def group_sum_speedzones_player(df):
    # cnt = df.groupby(['player_id', 'speedZone'], as_index=False).agg({'dist': sum, 'team': 'first', 'jersey_number': 'first'})
    df.dist = (df['dist'] / 1000).round(decimals=3)
    df['team'] = df['team'].replace([0, 1], ['CH', 'PT'])
    df['team-pl'] = df['team'].astype(str) + '-' + df['player'].astype(str)
    df['speedzone'] = df['speedzone'].astype(float)
    cnt = df.sort_values(by=['team', 'player', 'speedzone'])
    return cnt


# count different kind of events
def group_count_events_player(df):
    count_event = df.groupby(['player_id', 'event_class'], as_index=False).agg(
        {'percAcc': 'count', 'team': 'first', 'jersey_number': 'first'})
    count_event['team'] = count_event['team'].replace([0, 1], ['CH', 'PT'])
    count_event['team-pl'] = count_event['team'].astype(str) + '-' + count_event['jersey_number'].astype(str)
    count_event = count_event.sort_values(by=['team', 'jersey_number'])
    return count_event


def get_radio_buttons(df):
    radiodf = df.copy()
    radiodf['team'] = radiodf['team'].replace([0, 1], ['CH', 'PT'])
    radiodf['team-pl'] = radiodf['team'].astype(str) + '-' + radiodf['jersey_number'].astype(str)
    radiodf = radiodf.sort_values(by=['team', 'jersey_number'])
    return radiodf


# draw football pitch
def prepare_footballpitch(events):
    # open image and create plotly Figure
    # img = Image.open()
    fig = go.Figure()

    # draw players path in different colors according to the speedzone
    colors = ['cornflowerblue', 'lawngreen', 'yellow', 'orange', 'red']
    for df in events:
        for i in range(1, 6):
            fig.add_scattergl(x=df.x, y=df.y.where(df.speedzone == i), line={'color': colors[i - 1], 'width': 4},
                              name=i)

    # set size and range of plotly figure
    fig.update_layout(
        autosize=False,
        width=0.73 * 1163,
        height=0.7 * 783,
        xaxis=dict(visible=False, range=[-3, 110], showgrid=False),
        yaxis=dict(visible=False, range=[76, -4], showgrid=False),
        template="plotly_white",
        margin=dict(l=0, r=0, b=0, t=25, pad=0),
        legend=dict(orientation="h", y=1, x=0.045),
        legend_title_text="Speed Zones   "
    )

    # set size of the football pitch
    fig.add_layout_image(
        dict(source="assets/pitch.png",
             xref='x',
             yref='y',
             x=-3.8,
             y=-3.8,
             sizex=112.6,
             sizey=75.6,
             sizing='stretch',
             opacity=0.9,
             layer='below')
    )

    names = set()
    fig.for_each_trace(lambda trace: trace.update(showlegend=False) if (trace.name in names) else names.add(trace.name))

    return fig


# merge events and speed zones to get routes of the events per player
def get_events_routes_player(events, tracking, player):
    events = events[events['player_id'] == player]
    tracking = tracking[tracking['player_id'] == player]

    TIME_FORMAT = '%Y-%m-%d''T''%H:%M:%S.%f%z'
    events.copy().loc[:, 'ts_start'] = pd.to_datetime(events.copy().loc[:, 'ts_start'], format=TIME_FORMAT)
    events.copy().loc[:, 'ts_end'] = pd.to_datetime(events.copy().loc[:, 'ts_end'], format=TIME_FORMAT)
    tracking.copy().loc[:, 'timestamp'] = pd.to_datetime(tracking.copy().loc[:, 'timestamp'], format=TIME_FORMAT)

    events = events[events['percAcc'] > 10]

    # get x and y values of data of events
    list_df = []
    for index, row in events.iterrows():
        start = row['ts_start']
        end = row['ts_end']

        df = tracking[tracking['timestamp'].between(start, end)]
        if df.empty:
            pass
        else:
            list_df.append(df)

    final_df = pd.concat(list_df, ignore_index=True)
    # convert x and y values to new coordinate system
    final_df['x'] = final_df['x'] + 52.5
    final_df['y'] = final_df['y'] + 34

    # make list of data for different events
    final_df.loc[:, 'timestamp'] = pd.to_datetime(final_df.loc[:, 'timestamp'], format=TIME_FORMAT)
    # get time difference between tracking data. Difference greater than 0.04 means new events is occuring
    final_df['diff'] = final_df['timestamp'].diff().dt.total_seconds()
    # get index of data whith a different time difference than 0.04
    event_idx = list(final_df.index[final_df['diff'] != 0.04])
    event_list = []
    for i in range(len(event_idx) - 1):
        df = final_df.iloc[event_idx[i]:event_idx[i + 1], :]
        event_list.append(df)

    return event_list


df = get_df_from_redisdb()
e = get_sortedTable_fromDB(df)


def drawTable():
    return html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([html.H3('Events with highest percAcc in the last 2 minutes')]),
                dash_table.DataTable(
                    id='data-table',
                    data=e.to_dict('records'),
                    columns=[{"name": i, "id": i} for i in e.columns],
                    selected_rows=[],
                    style_data={
                        'color': 'black',
                        'backgroundColor': 'white'
                    },
                    style_data_conditional=[{
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(220, 220, 220)'
                    },
                        {
                            'if': {'filter_query': '{percAcc} > 180'},
                            'backgroundColor': '#FF0000',
                            'color': 'white'
                        },
                        {
                            'if': {'filter_query': '{percAcc} > 150 && {percAcc} < 180'},
                            'backgroundColor': '#FFA500',
                            'color': 'white'
                        }],
                    style_header={
                        'backgroundColor': 'rgb(210, 210, 210)',
                        'color': 'black',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                    style_cell={
                        'font_size': '12px',
                        'text_align': 'center'
                    },
                ), dcc.Interval(id='interval-table', interval=15 * 1000, n_intervals=0),
            ])
        )
    ])


def draw_speedZonesSum(df):
    return html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([html.H3('Covered distance in different speed zones')]),
                dcc.Graph(
                    figure=px.bar(df, x="team-pl", y="dist", color="speedzone",
                                  color_continuous_scale='Jet', text_auto=True
                                  ).update_layout(**layout_dict,
                                                  xaxis_type='category',
                                                  xaxis=dict(
                                                      tickmode='array',
                                                      tickvals=df['team-pl'].unique(),
                                                      ticktext=df['team-pl'].unique(),
                                                      title_text="Player (Team - Jersey number)"
                                                  ),
                                                  yaxis=dict(title_text="distance [km]")
                                                  ),
                    config={
                        'displayModeBar': False
                    }
                )
            ])
        ),
    ])


def draw_countEvents(df):
    return html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([html.H3('Summed events for different class events')]),
                dcc.Graph(
                    figure=px.bar(df, x="player_id", y="percAcc", color="event_class", text_auto=True,
                                  labels={'event_class': 'Detected event class'},
                                  color_discrete_sequence=px.colors.qualitative.Set1,
                                  ).update_layout(**layout_dict,
                                                  xaxis_type='category',
                                                  xaxis=dict(
                                                      tickmode='array',
                                                      tickvals=df['player_id'].unique(),
                                                      ticktext=df['team-pl'].unique(),
                                                      title_text="Player (Team - Jersey number)"),
                                                  yaxis=dict(title_text="Number of events"),
                                                  legend=dict(
                                                      orientation="h",
                                                      yanchor="bottom",
                                                      y=1.1,
                                                      x=0
                                                  )
                                                  ),
                    config={
                        'displayModeBar': False
                    }
                )
            ])
        ),
    ])


def draw_footballPitch_events(radio):
    return html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([html.H3('Detected events by player from the last 2 minutes'),
                          html.P('Selection: (Team - Jersey number)')]),
                dcc.RadioItems(
                    options=[{"label": radio[radio['player_id'] == i]['team-pl'].unique()[0], "value": i} for i in
                             radio['player_id'].unique()],
                    value=10,
                    id='selection-player-id',
                    labelStyle={'display': 'inline-block', 'padding': '12px'}
                ),
                dcc.Graph(
                    id='pitch-plot',
                    config={'displayModeBar': False},
                    style={'height': 520}
                )
            ])
        ),
    ])


def draw_barplot_percent():
    return html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([html.H3('Percentage of total percentage Acceleration by player')]),
                dcc.Graph(id="group-percAcc-percentage", config={'displayModeBar': False}),
            ])
        ),
    ])


@app.callback(
    [Output('data-table', 'data'),
     Output('group-percAcc-percentage', 'figure'),
     Output('tabs-content-bar-graphs', 'children'),
     Output('pitch-plot', 'figure')],
    [Input('interval-table', 'n_intervals'),
     Input('bar-plots-tabs', 'value'),
     Input('selection-player-id', 'value')])
def update_data(n, tab, player_id):
    events = get_df_from_redisdb()
    sz = get_sumSpeedZones_from_redisdb()

    # data table
    datatable = get_sortedTable_fromDB(events.copy())

    # events percentage
    events_perc = group_sum_percAcc_percent(events)
    fig2 = px.bar(events_perc, x='player_id', y='percAccperc', color='percAccperc', color_continuous_scale='Jet')
    fig2.update_layout(**layout_dict,
                       xaxis_type='category',
                       xaxis=dict(
                           tickmode='array',
                           tickvals=events_perc['player_id'].unique(),
                           ticktext=events_perc['team-pl'].unique(),
                           title_text="Player (Team - Jersey number)"),
                       yaxis=dict(title_text="portion on percentage acceleration [%]"),
                       margin=dict(l=0, r=0, b=0, t=0, pad=0),
                       bargap=0.5
                       ).update_coloraxes(showscale=False)

    # draw plots for tabs
    if tab == 'tab-1-speedzones':
        cnt = group_sum_speedzones_player(sz)
        figtab = draw_speedZonesSum(cnt)
    elif tab == 'tab-2-events':
        count_event = group_count_events_player(events)
        figtab = draw_countEvents(count_event)

    # draw pitch plot
    tracking = get_speedzones_from_redisdb()
    routes_events = get_events_routes_player(events=events.copy(), tracking=tracking.copy(), player=player_id)
    figure_plot = prepare_footballpitch(routes_events).update_layout(**layout_dict)

    return [datatable.to_dict('records'), fig2, figtab, figure_plot]


# Video synchronization with kafka messages
@app.callback(Output('video-player', 'seekTo'),
              Input('data-table', 'active_cell'))
def print_rows(active_cell):
    if active_cell is not None:
        df = get_df_from_redisdb()
        df = df.sort_values('ts_start')
        ts_event = df.tail(10).iloc[active_cell['row']]['ts_start']
        ts_video = get_timediff_event(START_TIME_VIDEO, ts_event) + 0.04
        return ts_video


# show video of the game
def draw_movie():
    return html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([
                    dash_player.DashPlayer(
                        id='video-player',
                        url='assets/game2.mp4',
                        controls=True,
                        width='100%',
                        height='100%'
                    )
                ])
            ])
        )
    ])


# main Layout of the Dash app
app.layout = html.Div([
    html.H2("Welcome to the Dashboard", style={'textAlign': 'center'}),
    dbc.Card(
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    drawTable()
                ], width=6),
                dbc.Col([
                    draw_movie()
                ], width=6),
            ], align='center'),
            html.Br(),
            dbc.Row([
                dbc.Col([
                    dcc.Tabs(id="bar-plots-tabs", value='tab-1-speedzones', children=[
                        dcc.Tab(label='Speed Zones', value='tab-1-speedzones'),
                        dcc.Tab(label='Detected Events', value='tab-2-events')
                    ]),
                    html.Div(id='tabs-content-bar-graphs')
                ], width=12),
            ], align='center'),
            html.Br(),
            dbc.Row([
                dbc.Col([
                    draw_footballPitch_events(get_radio_buttons(get_speedzones_from_redisdb()))
                ], width=7),
                dbc.Col([
                    draw_barplot_percent()
                ], width=5),
            ], align='center'),
        ]), color='dark'
    )
])

# host must be mapped to the localhost of Docker
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)