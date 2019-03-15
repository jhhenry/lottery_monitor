#!/usr/bin/env node

const meow = require('meow');
const reader = require('./event_reader');

const helpText = `
Run an app that read lottery contract's events from kafka and store them persistently to a database.
A assumption here is the topic has only one partition configured.
Options:
    --offset -o   offset since which the events in kafka are read. it can be a positive number or "beginning", end or stored.
                    if not set, the offset maintained by the kafka itself will be used.
    --kafka -k   kafka brokers list
    --topic -t   kafka topic
    --group -g   kafka consumer group id. default value: "defaultGroup"
`;
const options = {
    flags: {
        offset: {
            type: 'string',
            alias: 'o'
        },
        kafka: {
            type: 'string',
            alias: 'k'
        },
        topic: {
            type: 'string',
            alias: 't'
        },
        group: {
            type: 'string',
            alias: 'g'
        }
    }
}

const cli = meow(helpText, options);

const kafkaBrokers = cli.flags.kafka;
const topic = cli.flags.topic;
const offset = cli.flags.offset ? cli.flags.offset : -1;
const group = cli.flags.group ? cli.flags.group : "defaultGroup";
reader(kafkaBrokers, topic, group, isNaN(offset) ? offset : Number(offset));


