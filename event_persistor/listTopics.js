const Kafka = require('./kafka_client');
const testUtils = require('./test/testUtils');
const log = console.log;

const brokers = process.env.kafkaBrokers ? process.env.kafkaBrokers : "192.168.56.103:9092,192.168.56.103:9093,192.168.56.103:9094";
const consumer = Kafka.getConsumer("test_group", brokers);
const admin = Kafka.getAdminClient(brokers, 'admin_client');
log("consumer.seek", consumer.seek);
consumer.then(c => {
    c.getMetadata(null, (err, metadata) => {
        if (err) {
            console.error(err);
        } else {
            //console.log(metadata);
            const topics = metadata.topics;
            topics.forEach(t => {
                if (t.name == '__consumer_offsets') {
                    printOffset(t);
                } else {
                    const name = t.name;
                    log(`Topic: ${name}`);
                    // Object.keys(t).forEach(item => {
                    //     log(`   ${item}: ${t[item]}`);
                    // });
                }
               
            });
        }
        c.disconnect();
    });
   
});


function printOffset(offsets) {
    const partitions = offsets.partitions;
    //log(typeof partitions);
    partitions.forEach(p => {
        log(`-------------`);
        // Object.keys(p).forEach(item => {
        //     log(`   ${item}: ${p[item]}`);
        // });
    })
}