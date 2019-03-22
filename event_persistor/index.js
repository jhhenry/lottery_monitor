#!/usr/bin/env node

const meow = require('meow');
const reader = require('./event_reader');
const persit = require('./event_persist');

const helpText = `
Run an app that read lottery contract's events from kafka and store them persistently to a database.
A assumption here is the topic has only one partition configured.
Options:
    --offset -o   offset since which the events in kafka are read. it can be a positive number or "beginning", end or stored.
                    if not set, the offset maintained by the kafka itself will be used.
    --kafka -k   kafka brokers list
    --topic -t   kafka topic
    --group -g   kafka consumer group id. default value: "defaultGroup"
    --mysql -m   the mysql connection info formatted as <user>@<host>:<port>/<data>
    --pwd   -p   the password used for the mysql connection
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
        },
        mysql: {
            type: 'string',
            alias: 'm'
        },
        pwd: {
            type: 'string',
            alias: 'p'
        }
    }
}

const cli = meow(helpText, options);

const kafkaBrokers = cli.flags.kafka;
const topic = cli.flags.topic;
const offset = cli.flags.offset ? cli.flags.offset : -1;
const group = cli.flags.group ? cli.flags.group : "defaultGroup";
const mysql = cli.mysql;
const [user, host, port, database] = getMySqlParts(mysql);
if (user && host && port && database) {
    let pwd = cli.pwd;
    if (!pwd) {
        // input from the terminal
        getPwdPromise().then(pwd => {
            // test connection
            testConnection(host, port, user, pwd, database);
            // call reader
            reader(kafkaBrokers, topic, group, isNaN(offset) ? offset : Number(offset), host, port, user, pwd, database);
        }).catch(err => {
            throw new Error(err);
        })
    } else {
        //test connection
        testConnection(host, port, user, pwd, database);
        reader(kafkaBrokers, topic, group, isNaN(offset) ? offset : Number(offset), host, port, user, pwd, database);
    }
} else {
    throw new Error(`lack of mysql's connection info`);
}

function getMySqlParts(mysqlStr) {
    const regEx = /([^@]+)@([^:]+):(\d+)\/(.*)/;
    const matches = mysqlStr.match(regEx);
    if (matches && matches.size >= 5) {
        return matches.slice(1, 5);
    }
}

function getPwdPromise() {
    const rl = require('readline');
    const { Writable } = require('stream');
    var mutableStdout = new Writable({
        write: function (chunk, encoding, callback) {
            //console.log('I am here', this.muted);
            if (!this.muted)
                process.stdout.write(chunk, encoding);
            callback();
        }
    });
    mutableStdout.muted = false;

    const prompt = rl.createInterface(
        {
            input: process.stdin,
            output: mutableStdout,
            terminal: true
        }
    );
    return new Promise((resolve, reject) => {
        prompt.question('Please input the password for mysql: ', 
            answer => {
                if (answer) resolve(answer);
                else reject("passowrd is required.")
                prompt.close();
            }
        );
        mutableStdout.muted = true;
    }
    );
}

async function testConnection(host, port, user, pwd, database)
{
    const conn = await persit.createConnection(host, port, user, pwd, database);
    persit.disconnect(conn);
}
