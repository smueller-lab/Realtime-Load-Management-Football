#!/bin/sh

echo "Preparing Kafka topics"
sh ./topics.sh

echo "Flush redis database"
redis-cli -h 127.0.0.1 -p 6379 -a abc123! FLUSHALL

echo "Starting Kafka Consumers"

cd src/workers


python3 -m faust -A w_calcMetrics worker -l info --without-web &
python3 -m faust -A w_getStartEvents worker -l info --without-web &
python3 -m faust -A w_getEventData worker -l info --without-web &
python3 -m faust -A w_combEvents worker -l info --without-web &
python3 -m faust -A w_filterEvents worker -l info --without-web &
python3 -m faust -A w_classEvents worker -l info --without-web &
python3 -m faust -A w_calcPercAcc worker -l info --without-web &
python3 -m faust -A w_percAccToRedis worker -l info --without-web