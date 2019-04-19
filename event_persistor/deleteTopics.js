const Kafka = require('./kafka_client');
const testUtils = require('./test/testUtils');

const brokers = process.env.kafkaBrokers ? process.env.kafkaBrokers : Kafka.defaultBrokers;
const consumer = Kafka.getConsumer("delete_topics", brokers);
const admin = Kafka.getAdminClient(brokers, 'admin_client');
const white_list = ["kafka_test_16d0e7"];

consumer.then(c => {
    c.getMetadata(null, (err, metadata) => {
        if (err) {
            console.error(err);
        } else {
            console.log(metadata);
            const topics = metadata.topics;
            topics.forEach(t => {
                const name = t.name;
                if (white_list.includes(name)) {
                    return;
                }
                if (name.indexOf('event_reader_test') == 0) {
                    deleteTopic(name, admin);
                }
                if (name.indexOf('kafka_test') == 0) {
                    deleteTopic(name, admin);
                }
            });
        }
        c.disconnect();
    });
   
});

async function deleteTopic(topic, admin)
{
    await testUtils.deleteTopic(admin, topic);
}