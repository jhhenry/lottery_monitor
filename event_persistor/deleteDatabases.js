const persistor = require('./event_persist');
const util = require('util');
const exec = util.promisify(require('child_process').exec);


run();

async function run() {
    console.log("dropping databases");
    const dbs = await getAllDatabases();
    
    dbs.split('\n').filter(db => db.startsWith('test_')).forEach(e => {
        dropTestDatabase(e);
    });
}
async function getAllDatabases() {
    const showall = "show databases;"
    const dbs = await exec(`mysql -u root -pHjin_5105 -e "${showall}" `);
    return Promise.resolve(dbs.stdout);
}
async function dropTestDatabase(db) {
    const dropDatabase = "drop database " + db;
    console.log(`dropping database ${db}`);
    await exec(`mysql -u root -pHjin_5105 -e "${dropDatabase}" `);
}