const kafka_client = require("./kafka_client");
const {PersistentHandler} = require('./event_handler');
const log = console.log;

function run(kafkaBrokers, topic, group, offset, host, port, user, pwd, database,cb) {

    const consumer_p = kafka_client.getConsumer(group, kafkaBrokers);
    consumer_p.then(c => {
        const consumer = c;
        consumer.subscribe([topic]);
        if (offset != -1) {
            consumer.assign([{topic, partition: 0, offset}]);
        }
        consumer.consume();
        log(`start consuming on topic '${topic}' from the offset ${offset}, using the group ${group}`);
       
        //const dataHandler = cb ? cb : persistHandler.handleEvent;
        if (cb) {
            consumer.on("data", cb);
        } else {
            const persistHandler = new PersistentHandler(host, port, user, pwd, database);
            consumer.on("data", function(data) {
                // console.log(`${group} consumed message`);
                 persistHandler.handleEvent(data);
             });
        }
    });
}

module.exports = run;

// const group = "lottery_consumer_test1";
// const broker_list = "192.168.56.103:9092,192.168.56.103:9093,192.168.56.103:9094";
// const consumer_p = kafka_client.getConsumer(group, broker_list);
// const topic = "lottery-monitor-dev";

// consumer_p.then(c => {
//     const consumer = c;
//     consumer.subscribe([topic]);

//     log(`subscribed to the topic: ${topic}`);
//     var timeout = 5000, partition = 0;
//     consumer.queryWatermarkOffsets(topic, partition, timeout, function (err, offsets) {
//         if (err) {
//             log(err);
//             return;
//         }
//         var high = offsets.highOffset;
//         var low = offsets.lowOffset;
//         log(`highOffset: ${high}, lowOffset: ${low}`);
//     });
    
//     // consumer.commit(message);
//     // log("committed message");
//     log(`consumer.seek: ${consumer.seek}`);
//     consumer.consume();
//     log("called consume");
//     consumer.on("data", function(data) {
//         console.log('Message found!  Contents below.');
//         console.log(data)
//     });
//     log("called on data");
// })