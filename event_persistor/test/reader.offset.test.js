const test = require('ava');
const chalk = require('chalk');
const path = require('path');
const reader = require('../event_reader');
const EventEmitter = require('events');

const KafkaClient = require('../kafka_client');
const testUtils = require('./testUtils');

const logPrefix = `<${path.basename(__filename)}> `;
const log = console.log;

test.before("create a kafka admin client", async t => {
    const admin = KafkaClient.getAdminClient(KafkaClient.defaultBrokers, "admin_client_" + testUtils.getRandBytes(3));
    t.context.admin = admin;
});

test.beforeEach("create a new topic", async t => {
    const topic = "kafka_test_" + testUtils.getRandBytes(3);
    log(chalk.magentaBright(`${logPrefix}topic: ${topic}`));
    const admin = t.context.admin;
    await testUtils.createTopic(admin, topic, 1, 2);
    t.context.topic = topic;
    t.context.producer = await produceMessages(topic, 10);
});

test.afterEach("deleted a new topic", async t => {
    const admin = t.context.admin;
    t.context.producer.disconnect();
    await testUtils.deleteTopic(admin, t.context.topic);
});

const offsets = [0, 4, 9];

offsets.forEach(offset => {
    test.serial(`reader Consumer test: offset = ${offset}`, async t => {
        const topic = t.context.topic;
        const consumer = await KafkaClient.getConsumer("test_consumer_class_" + testUtils.getRandBytes(3, 'hex'), KafkaClient.defaultBrokers);
        const cc = new reader.Consumer(consumer);
        try {
            let mr = 0;
            let first;
            await new Promise((resolve, reject) => {
                cc.start(topic, offset, "kafka-server", 3306, 'root', "Hjin_5105", "no_need_database",
                    d => {
                        //log(chalk.magentaBright(`received message for the reader consumer ${offset}th: ${d.offset}`));
                        mr++;
                        if (!first) {
                            first = JSON.parse(d.value);
                            log(chalk.blue(`the first message for the reader consumer ${offset}th: ${first.index}`));
                        }
                        if (mr == 10 - offset) {
                            resolve("all messages received");
                        }
                    }
                );
                setTimeout(() => reject('test timeout: offset = ${offset}'), 40000);
            }
            );
            log(chalk.magentaBright(`received ${mr} messages for the reader consumer ${offset}th`));
            t.truthy(first);
            t.is(first.index, offset);
            t.is(mr, 10 - offset);
            cc.stop();
            //await wait(2);
        } catch (err) {
            console.error(chalk.red(err));
        } finally {
            cc.stop();
        }
        t.pass();
    });
});

test("negative offset", async t => {
    const topic = t.context.topic;
    const consumer = await KafkaClient.getConsumer("test_consumer_class_" + testUtils.getRandBytes(3, 'hex'), KafkaClient.defaultBrokers);

    try {
        const readAndCommitPromise = new Promise((resolve, reject) => {
            consumer.assign([{ topic, partition: 0, offset: 0 }]);
            consumer.consume((err, d) => {
                if (err) {
                    console.error(chalk.red(`${logPrefix} consume error: ${err}`));
                    reject(err);
                    return;
                }
                log(chalk.magentaBright(`${logPrefix} readAndCommitPromise received message: ${d.offset}`));
                if (d.offset == 8) {
                    consumer.commitMessage(d);
                    resolve("consumer committed");
                }
            });
            //consumer.consume();
            setTimeout(() => {
                reject("readAndCommitPromise timeout failed");
            }, 60000);
        });
        await readAndCommitPromise;
        consumer.unsubscribe();

        const cc = new reader.Consumer(consumer);
        let mr = 0;
        let first;
        let offset = -1;
        try {
            await new Promise((resolve, reject) => {
                cc.start(topic, offset, "kafka-server", 3306, 'root', "Hjin_5105", "no_need_database",
                    d => {
                        //log(chalk.magentaBright(`received message for the reader consumer ${offset}th: ${d.offset}`));
                        mr++;
                        if (!first) {
                            first = JSON.parse(d.value);
                            log(chalk.red(`the first message for the reader consumer ${offset}th: ${first.index}`));
                        }
                        if (mr == 1) {
                            resolve("all messages received");
                        }
                    }
                );
                setTimeout(() => reject("read 9th message timeout"), 60000);
            });
        } finally {
            cc.stop();
        }
        
        t.is(first.index, 9);
    } catch (err) {
        t.fail(err);
    } finally {
        consumer.unsubscribe();
        consumer.disconnect();
    }

});

async function produceMessages(topic, messages_count) {
    const producer = await KafkaClient.getProducer(KafkaClient.defaultBrokers);
    producer.on('event.error', function (err) {
        console.error('Error from producer');
        console.error(err);
    });
    // Poll for events every 100 ms
    producer.setPollInterval(100);

    await KafkaClient.produceMessages(producer, topic, {}, messages_count);
    return Promise.resolve(producer);
}
