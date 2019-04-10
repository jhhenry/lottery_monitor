const test = require('ava');
const chalk = require('chalk');
const log = console.log;
const KafkaClient = require('../kafka_client');
const testUtils = require('./testUtils');
const reader = require('../event_reader');

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

test.before('produce messages to kafka asynchronously', async t => {
    //create a new database for test
    await testUtils.createTestDatabase(t);

    // create a temp kafka topic
    console.log('before test');
    const admin = KafkaClient.getAdminClient(brokers, 'admin_client');
    t.context.admin = admin;
    const topic = 'event_reader_test_' + testUtils.getRandBytes(3, 'hex');
    t.context.topic = topic;
    await testUtils.createTopic(admin, topic, 1, 2);

    // constructs events messages
    const producer = await KafkaClient.getProducer(brokers);
    t.context.producer = producer;
    {
         // send them to the kafka topic
        console.log('start producing test messages to kafka');
        let count = 0;
        while (count < messages_count) {
            const e = {};
            Object.assign(e, event_template);
            e.transactionHash = testUtils.getRandBytes(32, 'hex');
            setTimeout(() => {
                producer.produce(topic, null, Buffer.from(JSON.stringify(e)), "test_event", Date.now());
                producer.flush(500, () => {
                    //log(chalk.yellow(`message flushed: ${r1}, ${r2}`));
                });
            }, count * 1000);
            count++;
        }
    };
});

test.after('close kafka admin client', async t => {
    console.log('test after');
    // delete the topic once all the tests completed
    const admin = t.context.admin;
    await testUtils.deleteTopic(admin, t.context.topic);
    await testUtils.dropTestDatabase(t);
    admin.disconnect();
    t.context.producer.disconnect();
});

test('successful consuming test', async t => {
    console.log('consumer test');
    const topic = t.context.topic; 
    const c = await KafkaClient.getConsumer("test_confirm_group" + testUtils.getRandBytes(4, 'hex'), brokers);
    const event_consumer = new reader.Consumer(c);
    let messages_handled = 0;
    event_consumer.on('event_handled', function (d){
        log(chalk.cyan(`received data in the event_handled`));
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
            setTimeout(reject, messages_count * 2 *1000);
        })
        //await waitForMessages(c, messages_count);
        const [event_rows, ] = await t.context.con.query("select id from events");
        const [redeemed_rows, ] = await t.context.con.query("select lottery_sig from redeemedInfo");
        t.is(event_rows.length, 10);
        t.is(redeemed_rows.length, 10);
        t.pass();
    } catch(err) {
        console.error(err);
        t.fail(err);
    } finally {
        //consumer.disconnect();
        event_consumer.stop();
    }
});

async function waitForMessages(consumer, messages_count)
{
    await new Promise((resolve, reject) => {
        //setTimeout(resolve, messages_count * 1500);
        let c = 0;
        const timeout = messages_count * 2.5 * 1000;
        consumer.on("event_handled", (d) => {
            // const event = JSON.parse(d.value);
            log(chalk.cyan(`received data in the waitForMessages`));
            c++;
            if (c == messages_count) {
                setTimeout(resolve, 2000);// the reader should have consumed all expected messages, too.   
            }
        });
        setTimeout(() => {
            reject("waitForMessages timeout")
        }, timeout);
    });
}

test.todo('restart the consumer & expect the correct offset');



