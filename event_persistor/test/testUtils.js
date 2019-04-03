const persistor = require('../event_persist');
const fs = require('fs');
const path = require('path');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const crypto = require('crypto');

async function createTestDatabase(t) {
    const newDatabaseName = 'test_' + getRandBytes(20);
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

async function createTopic(admin, topic, partition, replication_factor)
{
    return new Promise((resolve, reject) => {
        admin.createTopic({
            topic,
            num_partitions: partition,
            replication_factor
        }, function (err) {
            // Done!
            if (err) { reject(err);}
            console.log('new Topic created, ');
            setTimeout(resolve, 5000);
        })
    });
}

async function deleteTopic(admin, topic)
{
    await new Promise((resolve, reject) => {
        console.log(`deleting topic, "${topic}"`);
        // setTimeout(() => {
        admin.deleteTopic(topic, 1000, (err) => {
            if (err) {
                reject(err);
                return;
            }
            //console.log('topic deleted: ', r);
            resolve();
        }
        )
    //}
        // , 1000);
    });
}

function getRandBytes(num, encoding)
{
   return  crypto.randomBytes(num).toString(encoding ? encoding : 'hex');
}

module.exports.createTestDatabase = createTestDatabase;
module.exports.dropTestDatabase = dropTestDatabase;
module.exports.createTopic = createTopic;
module.exports.deleteTopic = deleteTopic;
module.exports.getRandBytes = getRandBytes;