const Kafka = require('node-rdkafka');
// console.log(Kafka.features);
// console.log(Kafka.librdkafkaVersion);
const defaultBrokers = "192.168.56.103:9092,192.168.56.103:9093,192.168.56.103:9094";

function getProducer(broker_list) {
    let producer = new Kafka.Producer({
        "client.id": "lottery_monitor",
        'metadata.broker.list': broker_list,
        'dr_cb': true
      });
    // Any errors we encounter, including connection errors
    
    // producer.on('delivery-report', function(err, report) {
    //     // Report of delivery statistics here:
    //     console.log(report);
    // });
    producer.connect({}, function(err) {
        if (err) {
            console.log(err);
        }
    })
    return new Promise(
        (resolve, reject) => {
            producer.on('ready', function(){
                console.log("enter producer ready")
                resolve(producer);
            });
            producer.on('event.error', function(err) {
                console.error('Error from producer');
                reject(err);
            });
        }
    );
}

function getConsumer(group, broker_list) {
    let consumer = new Kafka.KafkaConsumer({
        'group.id': group,
        'metadata.broker.list': broker_list,
        'enable.auto.commit': false,
    }, {
        'auto.offset.reset': 'earliest' // consume from the start
    });
    consumer.connect();
    return new Promise(
        (resolve, reject) => {
            consumer.on('ready', function() {
                console.log("enter consumer ready");
                resolve(consumer);
            });
            consumer.on('event.error', function(err) {
                console.error('Error from consumer');
                reject(err);
            });
    });
}

function getAdminClient(broker_list, client_id)
{
    if (broker_list && broker_list.length) {
    } else {
        broker_list = process.env.kafkaBrokers ? process.env.kafkaBrokers : defaultBrokers; 
    }
    
    return client = Kafka.AdminClient.create({
        'client.id': client_id,
        'metadata.broker.list': broker_list
      });
}



module.exports.getProducer = getProducer;
module.exports.getConsumer = getConsumer;
module.exports.getAdminClient = getAdminClient;
module.exports.defaultBrokers = defaultBrokers;