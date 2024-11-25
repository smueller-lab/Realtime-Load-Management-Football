-- ksqldb commands for speedzones

--SET 'auto.offset.reset' = 'earliest';

DROP TABLE IF EXISTS t_speedzones;
DROP TABLE IF EXISTS s_SpeedZones;

CREATE STREAM s_SpeedZones (
    timestamp VARCHAR,
    team BIGINT,
    jersey_number BIGINT,
    x DOUBLE,
    y DOUBLE,
    speed DOUBLE,
    speedZone BIGINT,
    dist DOUBLE,
    player_id BIGINT,
    acc_actual DOUBLE,
    acc_event DOUBLE,
    start_event BIGINT
) WITH (KAFKA_TOPIC='TrackingStartEvents',
    PARTITIONS=29,
    REPLICAS=3,
    VALUE_FORMAT='JSON',
    KEY_FORMAT='KAFKA');

CREATE TABLE t_speedzones
WITH (KAFKA_TOPIC='finalSpeedZones',
      REPLICAS=3,
      PARTITIONS=150,
      VALUE_FORMAT='JSON',
      KEY_FORMAT='JSON')
AS SELECT s.team, s.jersey_number, s.speedZone, SUM(s.dist) as total_distance
FROM s_SpeedZones s
GROUP BY s.team, s.jersey_number, s.speedZone EMIT CHANGES;