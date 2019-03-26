const persistor = require('../event_persist');
const fs = require('fs');
const path = require('path');
const util = require('util');
const exec = util.promisify(require('child_process').exec);

async function createTestDatabase(t) {
    const newDatabaseName = 'test_' + Date.now();
    //console.log(`t.context: ${t.context}`);
    t.context.newDatabaseName = newDatabaseName;
    const createDatabasesStr = "CREATE DATABASE IF NOT EXISTS " + newDatabaseName;

    await exec(`mysql -u root -pHjin_5105 -e "${createDatabasesStr}" `);
    //console.log(`create database result: ${result.stdout}`);
    const con = await persistor.createConnection('localhost', 3306, "root", 'Hjin_5105', newDatabaseName);
    t.context.con = con;
    const createStat = fs.readFileSync(path.resolve(__dirname, "..", "database", "schema.sql")).toString();
    const index = createStat.lastIndexOf('create table');
    //console.log(`createStat: ${createStat}`);
    await con.query(createStat.substr(0, index));
    await con.query(createStat.substr(index));
}

async function dropTestDatabase(t) {

    const newDatabaseName = t.context.newDatabaseName;
    const dropDatabase = "drop database " + newDatabaseName;
    await exec(`mysql -u root -pHjin_5105 -e "${dropDatabase}" `);
    //console.log(`drop database result: ${result}`);
    const con = t.context.con;
    await persistor.disconnect(con);
}

module.exports.createTestDatabase = createTestDatabase;
module.exports.dropTestDatabase = dropTestDatabase;