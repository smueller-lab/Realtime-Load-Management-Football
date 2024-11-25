#!/bin/bash
# script for creating Kafka topics

KAFKA=86.119.35.243:9092
N_PARTITIONS=29
N_REPLICA=3

docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --topic ingestion --delete --if-exists
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --topic Tracking --delete --if-exists
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --topic TrackingStartEvents --delete --if-exists
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --topic EventData --delete --if-exists
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --topic CombEvents --delete --if-exists
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --topic FilterEvents --delete --if-exists
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --topic ClassEvents --delete --if-exists
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --topic PercAcc --delete --if-exists


docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --create --topic ingestion --partitions $N_PARTITIONS --replication-factor $N_REPLICA
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --create --topic Tracking --partitions $N_PARTITIONS --replication-factor $N_REPLICA
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --create --topic TrackingStartEvents --partitions $N_PARTITIONS --replication-factor $N_REPLICA
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --create --topic EventData --partitions $N_PARTITIONS --replication-factor $N_REPLICA
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --create --topic CombEvents --partitions $N_PARTITIONS --replication-factor $N_REPLICA
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --create --topic FilterEvents --partitions $N_PARTITIONS --replication-factor $N_REPLICA
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --create --topic ClassEvents --partitions $N_PARTITIONS --replication-factor $N_REPLICA
docker exec -ti kafka-1 kafka-topics --bootstrap-server $KAFKA --create --topic PercAcc --partitions $N_PARTITIONS --replication-factor $N_REPLICA