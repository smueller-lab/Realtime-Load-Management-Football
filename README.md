# Realtime-Load-Management-Football

Python-based application to evaluate the external load of the players in real-time with Kafka.
Result dilvers real-time data about load of the players with:
- Event detection of sprinting events and percentage acceleration for each event
- Calculating covered distance by player and speed zone


## Data pipeline

1. Simulating the real-time stream with StreamSets
2. Event detection with Kafka
3. Sending event data, tracking data and covered distance by player and speed zone to Redis database
4. Real-time visualisation with Dash

![image](https://github.com/smueller-lab/Realtime-Load-Management-Football/blob/main/Kafka%20Architecture/Services.jpg?raw=true)


## Used services

- Docker
- Kafka, ksqldb
- Streamsets
- Python faust
- redis database
- Dash

## Kafka architecture

Topics are created with the ```topics.sh``` script and the Kafka consumers are written with faust agents [```/faust/workers```](/faust/workers)

![image](https://github.com/smueller-lab/Realtime-Load-Management-Football/blob/main/Kafka%20Architecture/final_kafka.jpg?raw=true)

## Starting the application

1. Go to the directory where the platform is located
2. Create docker container [```docker-compose up -d --build```]
3. Start in another terminal all faust worker [```bash start_workers.sh```], which also deletes
all data in the database. This script also starts another script [```topics.sh```] which creates all
Kafka topics.
4. Open StreamSets [```localhost:8682```]. Import pipeline [```simulator.json```]. Start simulator.
5. Start in first terminal the docker container [```docker run -p 8050:8050 <container
name>```]
6. Open Dash application [```localhost:8050```]

## Visualisation

**Left:** Sorted table with the events from the last two minutes with the highest percentage Acceleration.
<br>
**Right:** Video of the game

![image](https://github.com/smueller-lab/Realtime-Load-Management-Football/blob/main/Dashboard%20figures/dashboard1.png?raw=true)

Counted different events for each player

![image](https://github.com/smueller-lab/Realtime-Load-Management-Football/blob/main/Dashboard%20figures/dashboard2.png?raw=true)

Covered distance in different speedzones for all players.

![image](https://github.com/smueller-lab/Realtime-Load-Management-Football/blob/main/Dashboard%20figures/dashboard3.png?raw=true)

**Left:** Pitch with events from the last two minutes for different players in colors of the different speed zones
<br>
**Right:** Percentage of summed percentage Acceleration from total percentage Acceleration of all players.

![image](https://github.com/smueller-lab/Realtime-Load-Management-Football/blob/main/Dashboard%20figures/Dashboard5.jpg?raw=true)