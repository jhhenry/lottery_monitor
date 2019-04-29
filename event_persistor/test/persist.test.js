const test = require('ava');
const chalk = require('chalk');
const path = require('path');

const testUtils = require('./testUtils');
const persist = require('../event_persist');

const logPrefix = `<${path.basename(__filename)}> `;
const log = function(message) {console.log(`${chalk.magenta(logPrefix)}${chalk.cyan(message)}`);};

test.before('produce messages to kafka asynchronously', async t => {
    //create a new database for test
    await testUtils.createTestDatabase(t);
});

test.after('close kafka admin client', async t => {
    log('test after');
    await testUtils.dropTestDatabase(t);
});

test('test query non-existence kafka_event_received', async t => {
    // sconst newDatabaseNamet = t.context.newDatabaseName;
    const conn = t.context.con;
    const kafka_event = {
        topic: "test_group",
        partitionid: 0,
        consumer_group: "test_consumer",
        offset: 100
    };
    const [id, handling_state] = await persist.queryKafkaEventReceived(conn, kafka_event);
    t.falsy(id);
    t.falsy(handling_state);
});

test.serial('add a new kafka_event_received', async t => {
    // sconst newDatabaseNamet = t.context.newDatabaseName;
    const conn = t.context.con;
    const kafka_event = {
        topic: "test_group",
        partitionid: 0,
        consumer_group: "test_consumer",
        handling_state: 'processing',
        offset: 1
    };
    for(let i = 0; i < 3; i++) {
        const [id, handling_state] = await persist.recordKafkaEventReceived(conn, kafka_event);
        t.is(id, 1);
        t.is(handling_state, "processing");
        const [rows, ] = await conn.query('select * from kafka_events_received');
        t.is(rows.length, 1);
        t.is(rows[0].id, 1);
        t.is(rows[0].topic, kafka_event.topic);
        t.is(rows[0].consumer_group, kafka_event.consumer_group);
        t.is(rows[0].partitionid, kafka_event.partitionid);
    };
    // test another
    kafka_event.offset = 2;
    const [id, handling_state] = await persist.recordKafkaEventReceived(conn, kafka_event);
    t.is(id, 2);
    t.is(handling_state, "processing");
    const [rows, ] = await conn.query('select * from kafka_events_received');
    t.is(rows.length, 2);
    t.is(rows[1].id, 2);
});

test("record redeemed values", async t => {
    const eventValues = {
        blockNumber: 22233,
        event_type: "Redeemed",
        event_capture_time: "2019-01-01 22:22:22.222",
        txn: testUtils.getRandBytes(32, 'hex'),
        event_info: JSON.stringify({test:'test'})
    };
    const conn = t.context.con;
    try {
        await conn.beginTransaction();
        const eventResult = await conn.query('INSERT INTO events SET ?', eventValues);
        log(eventResult);
        const commitResult = conn.commit();
        await commitResult;
        const [event_rows, ] = await t.context.con.query("select id from events");
        t.is(event_rows.length, 1);
        t.pass();
    } catch (err) {
        t.fail(err);
        conn.rollback();
    }
})
