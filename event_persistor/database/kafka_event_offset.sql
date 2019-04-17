create table kafka_offset (
    topic VARCHAR(255),
    partitionid SMALLINT UNSIGNED,
    consumer_group VARCHAR(255),
    offset INT UNSIGNED,
    update_time TIMESTAMP(3),
    PRIMARY KEY(topic, partitionid, consumer_group)
);