const persistor = require('../event_persist');
const fs = require('fs');
const path = require('path');
const util = require('util');
const chalk = require('chalk');
const exec = util.promisify(require('child_process').exec);
const crypto = require('crypto');
const _ = require('lodash');

const logUtils = require("../logUtils")
const LOGGER = logUtils({ prefix: "<testUtils> ", style: "cyan" });
const log = console.log;

async function createTestDatabase(t) {
    const newDatabaseName = 'test_' + getRandBytes(5);
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

    //create kafka_event_received
    const createKafka = fs.readFileSync(path.resolve(__dirname, "..", "database", "kafka_events_received.sql")).toString();
    await con.query(createKafka);
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
            console.log(`new Topic "${topic}" created.`);
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

async function wait(second) {
    await new Promise((resolve,) => {
        const inter = setInterval(() => {
            log(chalk.magentaBright('waiting...'));
        }, 10000);
        setTimeout(() => {
            clearInterval(inter);
            resolve();
        }, second * 1000);
    });
}

/**
 * Compare two objects' common properties. If the values of their shared properties are all equal, return true.
 * Note: do not use options if you expect the compared objects are string, number, and other primitive types.
 * @param {object} obj 
 * @param {object} other 
 * @param {object} options propMap: Map, exclude: []
 */
function isEqual(obj, other, options) {
    if (!options && _.isEqual(obj, other)) return true;
    if(_.isNil(obj) && _.isNil(other)) return true;
    if ((!_.isNil(obj) && _.isNil(other)) || (_.isNil(obj) && !_.isNil(other))) return false;
    const stringToNum = (_.isNumber(obj) && _.isString(other)) || (_.isNumber(other) && _.isString(obj));
    if (stringToNum) {
        return _.toNumber(obj) == _.toNumber(other);
    }
    //LOGGER("comparing object: ", {obj, other, options});
    const exclude = options && options.exclude ? options.exclude : undefined;
    const pMap = options ? options.propMap : undefined;
    const [obj1, obj2] = _.isPlainObject(obj) && _.isPlainObject(other) ? [obj, other] : (!_.isPlainObject(obj) ? [obj, _.create(Object.getPrototypeOf(obj), other)] : [_.create(Object.getPrototypeOf(other), obj), other]);
    obj = recreateObject(obj1, pMap);
    other = obj2;
    return _.isEqualWith(obj, other, function (v1, v2, key, p, op, stack) {
        if (!key) return undefined;
        // if(exclude) LOGGER({exclude, key}, _.includes(exclude, key))
        if (exclude && _.includes(exclude, key)) {
            return true;
        }
        // LOGGER(`comparing key, "${key}": `, v1, ", ", v2 );
        return isEqual(v1, v2) == true
    });
}

function recreateObject(src, propMap) {
    if(_.isMap(propMap) && !_.isEmpty(propMap)) {
        const target = _.isPlainObject(src) ? {} : _.create(Object.getPrototypeOf(src));
        _.keysIn(src).forEach(key => {
            if (propMap.has(key)) {
                target[propMap.get(key)] = src[key];
            } else {
                target[key] = src[key];
            }
        })
        return target;
    } else {
        return src;
    }
}

module.exports.createTestDatabase = createTestDatabase;
module.exports.dropTestDatabase = dropTestDatabase;
module.exports.createTopic = createTopic;
module.exports.deleteTopic = deleteTopic;
module.exports.getRandBytes = getRandBytes;
module.exports.wait = wait;
module.exports.isEqual = isEqual;