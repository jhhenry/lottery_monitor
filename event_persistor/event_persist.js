const {EventEmitter} = require('events');
const mysql = require('mysql2/promise');
class ExceptionHandler extends EventEmitter {

}

async function createConnection(host, port, user, pwd, database) {
    // create the connection
    const connection = await mysql.createConnection({ host, port, user, password: pwd, database });
    connection.connect();
    return connection;
}

function createPool(host, port, user, pwd, database)
{
    const pool = mysql.createPool({host, port, user, password: pwd, database});
    return pool;
}

async function disconnect(connection) {
    await connection.end();
}

async function recordRedeemedEvent(connection, lottery_event, redeemedValues, kafka_event) {
    if (!kafka_event.id) throw Error('kafka_event must exist in the database');
    try {
        console.log(kafka_event.id);
        await connection.beginTransaction();
        // log('start inserting to events: ');
        const eventResult = await connection.query('INSERT INTO events SET ?', lottery_event);
        redeemedValues.id = eventResult[0].insertId;
        // log('start inserting to redeemedInfo');
        await connection.query('INSERT INTO redeemedInfo SET ?', redeemedValues);
        await connection.query('UPDATE kafka_events_received SET handling_state = "stored" WHERE id = ?', [kafka_event.id]);
        const commitResult = connection.commit();
        return await commitResult;
    } catch(err) {
        //console.error("recordRedeemedEvent failed: ", err);
        connection.rollback();
        if (err.code && err.code == 'ER_DUP_ENTRY') {
            // two possible causes of duplicate: 
            // 1. INSERT INTO events SET: two distinct kafka event record the same lottery event
            // 2. INSERT INTO kafka_events_received: a kafka event were processed twice
            // in both cases, we should set the handling_state to "stored" to indicate 
            await connection.query('UPDATE kafka_events_received SET handling_state = "stored" WHERE id = ?', [kafka_event.id]);
        }
        throw err;
    }
}

async function recordKafkaEventReceived(connection, kafka_event)
{
    const [id, handling_state] = await queryKafkaEventReceived(connection, kafka_event);
   
    if(!id) {
        try {
            await connection.beginTransaction();
            const eventResult = await connection.query('INSERT INTO kafka_events_received SET ?', kafka_event);
            await connection.commit();
            return Promise.resolve([eventResult[0].insertId, kafka_event.handling_state]);
        } catch (err) {
            console.error("recordKafkaEventReceived error: ", err);
            connection.rollback();
            throw err;
        }
    } else {
        return Promise.resolve([id, handling_state]);
    }
}

async function queryKafkaEventReceived(connection, kafka_event) {
    try {
        const [rows,] = await connection.query('select id, handling_state from kafka_events_received where topic = ? and partitionid = ? and consumer_group = ? and offset = ?', 
            [kafka_event.topic, kafka_event.partitionid, kafka_event.consumer_group, kafka_event.offset]);
       if(rows.length > 0) {
           return Promise.resolve([rows[0].id, rows[0].handling_state]);
       } else {
            return Promise.resolve([undefined, undefined]);
       }
       
    } catch (err) {
        console.error(err);
        throw err;
    }
}

module.exports.createPool = createPool;
module.exports.createConnection = createConnection;
module.exports.recordRedeemedEvent = recordRedeemedEvent;
module.exports.queryKafkaEventReceived = queryKafkaEventReceived;
// module.exports.queryOffsetOfKafkaEvents = queryOffsetOfKafkaEvents;
module.exports.recordKafkaEventReceived = recordKafkaEventReceived;
module.exports.disconnect = disconnect;