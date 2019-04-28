create table kafka_events_received (
    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    topic VARCHAR(255),
    partitionid SMALLINT UNSIGNED,
    consumer_group VARCHAR(255),
    offset INT UNSIGNED,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    handling_state ENUM("processing", "stored"), 
    PRIMARY KEY(id),
    UNIQUE(consumer_group, topic, partitionid, offset)
);