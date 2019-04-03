const test = require('ava');
const chalk = require('chalk');
const path = require('path');
const EventEmitter = require('events');

const KafkaClient = require('../kafka_client');
const testUtils = require('./testUtils');
const reader = require('../event_reader');

const logPreix = `<${path.basename(__filename)}> `;
const log = console.log;

test.before('start inserting events to kafka', async t => {
    const admin = KafkaClient.getAdminClient(null, "reader_offset_client");
    t.context.admin = admin;
    const topic = 'event_reader_test_' + Date.now();
    t.context.topic = topic;
    await testUtils.createTopic(admin, topic, 1, 3);
    log(chalk.yellow(`${logPreix} created kafka topic: ${topic} `));
});

test.after.always('delete kafka topic', async t => {
    const admin = t.context.admin;
    const topic = t.context.topic;
    await testUtils.deleteTopic(admin, topic);
    log(chalk.yellow(`${logPreix} deleted kafka topic: ${topic} `));
    admin.disconnect();
});

test(chalk.bgBlue('start from the very beginning of the topic'), async t => {
    const producer = await KafkaClient.getProducer(KafkaClient.defaultBrokers);
    // send them to the kafka topic
    const topic = t.context.topic;
    log(chalk.magentaBright('start producing test messages to kafka'));
    const messags_count = 10;
    let count = 0;
    let count_sent = 0;
    const ee = new EventEmitter;
    ee.on('allSent', e => {
        log(chalk.magentaBright('received allSent event.'));
        producer.disconnect();
    })
    while (count < messags_count) {
        const e = {};
        e.index = count + 1;
        setTimeout(() => {
            producer.produce(topic, null, Buffer.from(JSON.stringify(e)), "test_event", Date.now());
            //producer.flush();
            count_sent++;
            if (count_sent == messags_count) {
                ee.emit('allSent');
            }
        }, count * 1000);
        count++;
    }
    let receivedCount = 0;
    try {
        await new Promise((resolve, reject) => {
            reader(KafkaClient.defaultBrokers, topic, 'test_group', 0, 'localhost', 3306, 'root', 'Hjin_5105', "no_need_database", data => {
                const e = JSON.parse(data.value);
               // log(chalk.magentaBright(`received event: ${e}, ${this}`));
                receivedCount++;
                if (receivedCount == 1 && e.index == 1) {
                    resolve();
                } else {
                    reject("the first event received is not expected");
                }
            });
        });
        t.pass();
    } catch(e) {
        t.fail();
    } finally {
        
    }
    
});

test.todo('start from a positive integer');

test.todo('reader restart from the kafka offset');

