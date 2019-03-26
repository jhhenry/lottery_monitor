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

module.exports.createPool = createPool;
module.exports.createConnection = createConnection;
module.exports.recordRedeemedEvent = recordRedeemedEvent;
module.exports.disconnect = disconnect;