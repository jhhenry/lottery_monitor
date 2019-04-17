const test = require('ava');
const chalk = require('chalk');
const path = require('path');
const util = require('util');

const KafkaClient = require('../kafka_client');
const testUtils = require('./testUtils');
const reader = require('../event_reader');

const logPrefix = `<${path.basename(__filename)}> `;
const log = function (message) { console.log(chalk.magenta(logPrefix), chalk.cyan(message)); };

const brokers = process.env.kafkaBrokers ? process.env.kafkaBrokers : KafkaClient.defaultBrokers;
const event_template = {
    address: "0x6dEFcB6F97E4b9765B88ebcaAF8A98f6338571f3",
    blockNumber: 4223,
    transactionHash: "0xb2b101a14487dd45c6861d6da2f7bea2de13b45af6edb2731f748bf4f41d3b98",
    transactionIndex: 0,
    blockHash: "0x2d841ffd9e1ecd8a95a144c5914ab93653f5ce71905bb040604ca7fd245d5f20",
    logIndex: 2,
    removed: false,
    id: "log_0x692da4314736bc80ca9c51c27040e95f8686e13d64fb8a97a4a408294eca62c0",
    returnValues: {
        0: "0x01143936633434376436383436346534303535636330c659068887db7414ef1a704b2159cfb75ba6396140ca8ed45ce07fea99a05c6fbd6d6abfba84efe632dd6e93ceef88a7f8333bfd00000166af0bd869f1a0abc06e0e603c6f3130ec27c58057b7d7b11d00000000000000000000000000000000000000000000000000000000000000320301",
        1: "0x3939313439356561356562373062343839386638",
        2: "0x3936633434376436383436346534303535636330",
        3: "50",
        4: "0xf1A0aBC06e0e603C6F3130eC27C58057b7D7B11D",
        5: "0x7c8c7A481a3dAc2431745Ce9b18B3BB8b6C526e7",
        6: "0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD",
        7: "0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD",
        lottery: "0x01143936633434376436383436346534303535636330c659068887db7414ef1a704b2159cfb75ba6396140ca8ed45ce07fea99a05c6fbd6d6abfba84efe632dd6e93ceef88a7f8333bfd00000166af0bd869f1a0abc06e0e603c6f3130ec27c58057b7d7b11d00000000000000000000000000000000000000000000000000000000000000320301",
        rs1: "0x3939313439356561356562373062343839386638",
        rs2: "0x3936633434376436383436346534303535636330",
        faceValue: "50",
        token_address: "0xf1A0aBC06e0e603C6F3130eC27C58057b7D7B11D",
        issuer: "0x7c8c7A481a3dAc2431745Ce9b18B3BB8b6C526e7",
        winner: "0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD",
        sender: "0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD"
    },
    event: "RedeemedLotttery",
    signature: "0x225668dc003423e36bd8d7793625f5e7b40fd2e20a878f131af633c1d6cc88a8",
    raw: {
        data: "0x000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000001c000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000032000000000000000000000000f1a0abc06e0e603c6f3130ec27c58057b7d7b11d0000000000000000000000007c8c7a481a3dac2431745ce9b18b3bb8b6c526e7000000000000000000000000bd6d6abfba84efe632dd6e93ceef88a7f8333bfd000000000000000000000000bd6d6abfba84efe632dd6e93ceef88a7f8333bfd000000000000000000000000000000000000000000000000000000000000008801143936633434376436383436346534303535636330c659068887db7414ef1a704b2159cfb75ba6396140ca8ed45ce07fea99a05c6fbd6d6abfba84efe632dd6e93ceef88a7f8333bfd00000166af0bd869f1a0abc06e0e603c6f3130ec27c58057b7d7b11d000000000000000000000000000000000000000000000000000000000000003203010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014393931343935656135656237306234383938663800000000000000000000000000000000000000000000000000000000000000000000000000000000000000143936633434376436383436346534303535636330000000000000000000000000",
        topics: [
            "0x225668dc003423e36bd8d7793625f5e7b40fd2e20a878f131af633c1d6cc88a8"
        ]
    },
    lottery_sig: "0x249228fe824c2d1429e8ea640cb5a6d20bae05306fe3e5d5f937a4cd0df0a1084a49d1d20ce6a12d270e8a507a3906d22df17f1af603dbfa07883747d83cf2251b",
    capture_time: "2019-01-01 22:22:22.222"
};

const messages_count = 10;

test.before('create a temp database and tables for test', async t => {
    //create a new database for test
    const admin = KafkaClient.getAdminClient(brokers, 'admin_client');
    t.context.admin = admin;
    const producer = await KafkaClient.getProducer(brokers);
    t.context.producer = producer;
});

test.after("close kafka admin and producer & drop the temp database", async t => {
    t.context.admin.disconnect();
    t.context.producer.disconnect();

});

test.beforeEach('create kafka topic', async t => {
    log("before each")
    await testUtils.createTestDatabase(t);
    const admin = t.context.admin;
    const topic = 'event_reader_test_' + testUtils.getRandBytes(3, 'hex');
    t.context.topic = topic;
    await testUtils.createTopic(admin, topic, 1, 2);
    sendKafkaMessages(t, messages_count);
});

test.afterEach('close kafka admin client', async t => {
    // delete the topic once all the tests completed
    const admin = t.context.admin;
    await testUtils.deleteTopic(admin, t.context.topic);
    await testUtils.dropTestDatabase(t);
});

test('successful consuming test', async t => {
    log('consumer test');
    const topic = t.context.topic;
    const c = await KafkaClient.getConsumer("test_confirm_group" + testUtils.getRandBytes(4, 'hex'), brokers);
    const event_consumer = new reader.Consumer(c);
    let messages_handled = 0;
    event_consumer.on('event_handled', function (d) {
       // log(`received data in the event_handled`);
        messages_handled++;
    });
    event_consumer.start(topic, 0, 'localhost', 3306, 'root', 'Hjin_5105', t.context.newDatabaseName);

    log(`subscribed to the topic: ${topic}`);
    try {
        // reader(brokers, topic, 'test_group', -1, 'localhost', 3306, 'root', 'Hjin_5105', t.context.newDatabaseName);
        await new Promise((resolve, reject) => {
            const iid = setInterval(() => {
                if (messages_handled == messages_count) {
                    clearInterval(iid)
                    resolve();
                }
            }, 1000);
            setTimeout(reject, messages_count * 2 * 1000);
        });
        await testUtils.wait(3);
        //await waitForMessages(c, messages_count);
        const [event_rows,] = await t.context.con.query("select id from events");
        const [redeemed_rows,] = await t.context.con.query("select lottery_sig from redeemedInfo");
        const [kafka_rows,] = await t.context.con.query('select id from kafka_events_received');
        t.is(event_rows.length, 10);
        t.is(redeemed_rows.length, 10);
        t.is(kafka_rows.length, 10);
        t.pass();
    } catch (err) {
        console.error(err);
        t.fail(err);
    } finally {
        //consumer.disconnect();
        event_consumer.stop();
    }
});

test('restart the consumer & use the offset stored in the database', async t => {
    // The monitor consistently sends events to the kafka topic
    // The consumer & persistor consumes the first 10 events then crashes.
    // The admin restarts the consumer using the same group 
    // & the offset stored in the mysql to catch up the latest events
    // meanwhile avoid as much duplicate messages as possible
    const topic = t.context.topic;
    const conn = t.context.con;
    const consumer_group = "test_confirm_group" + testUtils.getRandBytes(4, 'hex');
    try {
        const c = await KafkaClient.getConsumer(consumer_group, brokers);
        const event_consumer = new reader.Consumer(c);
        let messages_handled = 0;
        let stopped = false;
        let disconnected = false;
        event_consumer.on('event_handled', function () {
            //log(`received data in the event_handled`);
            messages_handled++;
            if (messages_handled >= 10 && !stopped) {
                event_consumer.stop();
                c.disconnect();
                log("called stop");
                log(`c.isconnected: ${c.isConnected()}`);
                stopped = true;
            }
        });
        c.on('disconnected', function (r, err) {
            if (err) {
                console.error(err);
                throw err;
            }
            disconnected = true;
            log("consumer disconnected");
        })
        
        event_consumer.start(topic, 0, 'localhost', 3306, 'root', 'Hjin_5105', t.context.newDatabaseName);
        const stopPromise = new Promise((resolve, reject) => {
            const iid = setInterval(function () {
                if ((!c.isConnected() || disconnected) && stopped) {
                    clearInterval(iid);
                    resolve();
                } 
            }, 500);
            setTimeout(reject, 20000);
        })
        const sendPromise = sendKafkaMessages(t, 50, {
            interval: 200 // 1 second
        })
        //log("will pass");
        log('waiting for disconnected');
        await stopPromise;
        log(`c.isconnected: ${c.isConnected()}`);
    
        // start a new consumer & reader
        const c2 = await KafkaClient.getConsumer(consumer_group, brokers);
        const event_consumer2 = new reader.Consumer(c2);
    
        // get new offset
        let newOffset = 0;
        const [maxOffsets, ] = await conn.query('select max(offset) from kafka_events_received where topic = ? and partitionid = ? and consumer_group = ? and handling_state="stored"', [topic, 0, consumer_group]);
        newOffset = maxOffsets[0]['max(offset)'] + 1;
        log(`newOffset: ${newOffset}`);
    
        event_consumer2.start(topic, newOffset, 'localhost', 3306, 'root', 'Hjin_5105', t.context.newDatabaseName);
        log("sening another X messages");
        await sendPromise;
        await testUtils.wait(5);
        
        const [event_rows,] = await conn.query("select id from events");
        const [redeemed_rows,] = await conn.query("select lottery_sig from redeemedInfo");
        const [kafka_rows,] = await conn.query('select id from kafka_events_received');
        //log(`event_rows: ${event_rows.length}, redeemed_rows: ${redeemed_rows.length}, kafka_rows: ${kafka_rows.length}`);
        t.is(event_rows.length, 60);
        t.is(redeemed_rows.length, 60);
        t.is(kafka_rows.length, 60);
        event_consumer2.stop();
    } catch(err) {
        t.fail(err);
    }
   

});

function sendKafkaMessages(t, count, options) {
    return KafkaClient.produceMessages(t.context.producer, t.context.topic, event_template, count, e_t => {
        const e = {};
        Object.assign(e, e_t);
        e.transactionHash = testUtils.getRandBytes(32, 'hex');
        return e;
    }, options);
}

test.todo('restart the consumer & use the committed offset in the kafka topic partition');




