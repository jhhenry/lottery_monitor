const mysql = require('mysql2/promise');
const log = console.log;

async function createConnection(host, port, user, pwd, database) {
    // create the connection
    const connection = await mysql.createConnection({ host, port, user, password: pwd, database });
    connection.connect();
    log(`called mysql connect`);
    return connection;
}

async function createPool(host, port, user, pwd, database)
{
    const pool = await mysql.createPool({host, port, user, password: pwd, database});
    return pool;
}

async function getConnectionFromPool(pool) {
    return await pool.getConnection();
}

async function disconnect(connection) {
    await connection.end();
}

async function recordRedeemedEvent(connection, eventValues, redeemedValues) {
    try {
        await connection.beginTransaction();
        //log('beginResult: ', beginResult);
        const eventResult = await connection.query('INSERT INTO events SET ?', eventValues);
        //log('eventResult: ', eventResult);
        redeemedValues.id = eventResult[0].insertId;
        //const redeemedInfoResult = 
        await connection.query('INSERT INTO redeemedInfo SET ?', redeemedValues);
        //log('redeemedInfoResult: ', redeemedInfoResult);
        const commitResult = connection.commit();
        //log('commitResult', commitResult);
        await commitResult;
    } catch(err) {
        connection.rollback();
        throw err;
    }
}

module.exports.createPool = createPool;
module.exports.getConnectionFromPool = getConnectionFromPool;
module.exports.createConnection = createConnection;
module.exports.recordRedeemedEvent = recordRedeemedEvent;
module.exports.disconnect = disconnect;