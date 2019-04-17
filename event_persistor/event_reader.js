const EventEmitter = require('events');
const path = require('path');
const LOGGER = require('./logUtils')({prefix: `<${path.basename(__filename)}> `, style: "bgBlue"});

const kafka_client = require("./kafka_client");
const {PersistentHandler} = require('./event_handler');

class Consumer extends EventEmitter{
    constructor(c) {
        super();
        this.consumer = c;
    }

    start(topic, offset, host, port, user, pwd, database,cb) {
       
        //log(chalk.magentaBright(`subscribed the topic: ${topic}`));
        if (isNaN(offset) || offset < 0) {
           //read from the latest offset committed to kafka
           this.consumer.subscribe([topic]);
           
        } else {
            //log(`call this.consumer.assign offset: ${offset}`);
            this.consumer.assign([{ topic, partition: 0, offset }]);
        }
        
        LOGGER(`start consuming on topic '${topic}' from the offset ${offset}`);
        
        if (!cb) {
            const persistHandler = new PersistentHandler(this.consumer.globalConfig['group.id'], host, port, user, pwd, database);
            cb = function(d) {
                try {
                    persistHandler.handleEvent(d);
                } catch(err) {
                    console.error('persistHandler error:', err);
                }
            };
        };
        this.consumer.on('data', (d) => {
            cb(d);
            this.emit("event_handled", d);
        });
        this.consumer.consume();
        // this.consumer.consume((err, d) => {
        //     if (err) {
        //         console.error("kafka consuming error: ", err, d)
        //     } else {
        //         // LOGGER(`the offset of the message received: ${d.offset}, ${this}`);
        //         cb(d);
        //         this.emit("event_handled", d);
        //     }
        // });
       
    }

    stop() {
        this.consumer.unsubscribe();
        this.consumer.disconnect();
    }
}

function run(kafkaBrokers, topic, group, offset, host, port, user, pwd, database,cb) {

    const consumer_p = kafka_client.getConsumer(group, kafkaBrokers);
    consumer_p.then(c => {
        const consumer = c;
        consumer.subscribe([topic]);
        if (offset != -1) {
            consumer.assign([{topic, partition: 0, offset}]);
        }
        consumer.consume();
        LOGGER(`start consuming on topic '${topic}' from the offset ${offset}, using the group ${group}`);
        //const dataHandler = cb ? cb : persistHandler.handleEvent;
        if (cb) {
            consumer.on("data", cb);
        } else {
            const persistHandler = new PersistentHandler(host, port, user, pwd, database);
            // log(`will use PersistentHandler to handle kafka events`);
            consumer.on("data", function(data) {
                //log(`${group} consumed message`);
                try {
                    persistHandler.handleEvent(data);
                } catch(err) {
                    console.error('persistHandler error:', err);
                }
                
            });
        }
    });
}

module.exports = run;
module.exports.Consumer = Consumer;
