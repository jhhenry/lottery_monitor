const mysql = require('mysql2/promise');
const log = console.log;

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

async function recordRedeemedEvent(connection, eventValues, redeemedValues) {
    //check if the lottery signature is duplicate
    
    try {
        await connection.beginTransaction();
        //log('beginResult: ', beginResult);
        const eventResult = await connection.query('INSERT INTO events SET ?', eventValues);
        //log('eventResult: ', eventResult);
       // const [r, ] = await connection.query('SELECT id FROM redeemedInfo WHERE lottery_sig = ?', redeemedValues.lottery_sig);
       // if(r.length <= 0) {
            redeemedValues.id = eventResult[0].insertId;
            await connection.query('INSERT INTO redeemedInfo SET ?', redeemedValues);
        //}       
        //log('redeemedInfoResult: ', redeemedInfoResult);
        const commitResult = connection.commit();
        //log('commitResult', commitResult);
        return await commitResult;
    } catch(err) {
        connection.rollback();
        throw err;
    }
}

async function recordKafkaEventReceived(connection, kafka_event)
{
    const [rows,] = await queryKafkaEventReceived(connection, kafka_event);
    if(rows.length == 0) {
        try {
            await connection.beginTransaction();
            const eventResult = await connection.query('INSERT INTO kafka_events_received SET ?', kafka_event);
            await connection.commit();
            return Promise.resolve([eventResult[0].insertId, kafka_event.handling_state]);
        } catch (err) {
            connection.rollback();
            throw err;
        }
    } else {
        let r = rows[0];
        return Promise.resolve([r.id, r.handling_state]);
    }
}

async function queryKafkaEventReceived(connection, kafka_event) {
    try {
        const [rows, cols] = await connection.query('select id, handling_state from kafka_events_received where topic = ? and partitionid = ? and consumer_group = ? and offset = ?', 
            [kafka_event.topic, kafka_event.partitionid, kafka_event.consumer_group, kafka_event.offset]);
       return Promise.resolve([rows, cols]);
    } catch (err) {
        throw err;
    }
}

module.exports.createPool = createPool;
module.exports.createConnection = createConnection;
module.exports.recordRedeemedEvent = recordRedeemedEvent;
module.exports.queryKafkaEventReceived = queryKafkaEventReceived;
module.exports.recordKafkaEventReceived = recordKafkaEventReceived;
module.exports.disconnect = disconnect;