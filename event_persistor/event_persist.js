const mysql = require('mysql2/promise');

const log = console.log;

async function createConnection(host, port, user, pwd, database) {
    // create the connection
    const connection = await mysql.createConnection({ host, port, user, password: pwd, database });
    connection.connect();
    log(`called mysql connect`);
    return connection;
}

async function disconnect(connection) {
    await connection.end();
}

async function recordRedeemedEvent(connection, eventValues, redeemedValues) {
    try {
        

        const beginResult = await connection.beginTransaction();
        log('beginResult: ', beginResult);
        const eventResult = await connection.query('INSERT INTO events SET ?', eventValues);
        log('eventResult: ', eventResult);
        redeemedValues.id = eventResult[0].insertId;
        const redeemedInfoResult = await connection.query('INSERT INTO redeemedInfo SET ?', redeemedValues);
        log('redeemedInfoResult: ', redeemedInfoResult);
        const commitResult = connection.commit();
        log('commitResult', commitResult);
        await commitResult;
    } catch(err) {
        connection.rollback();
        throw err;
    }
    // connection.beginTransaction(function (err) {
    //     if (err) { throw err; }
    //     /*
    //     txn CHAR(66) NOT NULL,
    //     blockNumber SMALLINT UNSIGNED,
    //     ts TIMESTAMP NOT NULL,
    //     event_type VARCHAR(30) NOT NULL,
    //     event_info JSON NOT NULL,
    //     */

    //     connection.query('INSERT INTO events SET ?', eventValues, function (error, results, fields) {
    //         if (error) {
    //             return connection.rollback(function () {
    //                 throw error;
    //             });
    //         }

    //         var log = 'Post ' + results.insertId + ' added';

    //         connection.query('INSERT INTO  SET data=?', log, function (error, results, fields) {
    //             if (error) {
    //                 return connection.rollback(function () {
    //                     throw error;
    //                 });
    //             }
    //             connection.commit(function (err) {
    //                 if (err) {
    //                     return connection.rollback(function () {
    //                         throw err;
    //                     });
    //                 }
    //                 console.log('success!');
    //             });
    //         });
    //     });
    // });
}

module.exports.createConnection = createConnection;
module.exports.recordRedeemedEvent = recordRedeemedEvent;