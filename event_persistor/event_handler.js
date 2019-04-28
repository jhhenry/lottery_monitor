const event_persist = require('./event_persist');
const path = require('path');
const LOGGER = require('./logUtils')({prefix: `<${path.basename(__filename)}> `, style: "bgBlue"});

class PersistentHandler {

    constructor(consumer_group, host, port, user, pwd, database) {
        this.consumer_group = consumer_group;
        this.pool = event_persist.createPool(host, port, user, pwd, database);
    }

    async handleEvent(kafka_msg) {
        if (!kafka_msg) {
            return;
        }
        //log(`getConnection: ${this.pool_p.getConnection}`)
        const kafka_event_val = JSON.parse(kafka_msg.value);
        //log(`handling event: ${event.event}`);
        try {
            if (kafka_event_val.event == 'RedeemedLotttery') {
                //log('handling the RedeemedLotttery: ', event.transactionHash);
                const p = await this.pool;
                const conn = await p.getConnection();
                try {
                    // stored the kafka message to the event_received table and
                    // set "processing" to the "handling_state" field.
                    // if the table already hold the message and the state is "stored"
                    // then skip  the following "recordRedeemedEvent" operation.
                    //await storeEventReceived
                    const kafka_event = PersistentHandler.constructKafaEventRec(kafka_msg, this.consumer_group, "processing");
                    let [id, handling_state] = await event_persist.queryKafkaEventReceived(conn, kafka_event);
                    if (handling_state == "stored") {
                        LOGGER(`recordKafkaEventReceived result: , ${[id, handling_state]}`);
                        return;
                    } else {
                        if (!id) {
                            [id, ] = await event_persist.recordKafkaEventReceived(conn, kafka_event);
                        }
                        kafka_event.id = id;
                        const eventValues = PersistentHandler.constructEventRec(kafka_event_val);
                        const redeemedValues = PersistentHandler.constructRedeemedRec(kafka_event_val);
                        
                        return await event_persist.recordRedeemedEvent(conn, eventValues, redeemedValues, kafka_event);
                    }
                } catch(err) {
                    if (err.code && err.code == 'ER_DUP_ENTRY') {
                        // TODO: send to a specific topic of the kafka
                        //LOGGER('got dup error');
                    } else {
                        console.error(err);
                        throw err;
                    }
                } finally {
                    conn.release();
                }
            }
        } catch (err) {
            console.error(err);
            throw err;
        }
    }

    static constructRedeemedRec(kafka_event_val)
    {
        return {
            beneficiary: kafka_event_val.returnValues.winner,
            lottery_sig: kafka_event_val.lottery_sig,
            payer: kafka_event_val.returnValues.issuer,
            rs1: kafka_event_val.returnValues.rs1,
            rs2: kafka_event_val.returnValues.rs2,
            sender: kafka_event_val.returnValues.sender
        };
    }

    static constructEventRec(kafka_event_val) {
        return {
            blockNumber: kafka_event_val.blockNumber,
            event_type: kafka_event_val.event,
            event_capture_time: kafka_event_val.capture_time,
            txn: kafka_event_val.transactionHash,
            event_info: JSON.stringify(kafka_event_val)
        };
    }

    static constructKafaEventRec(kafka_msg, consumer_group, handling_state) {
        return {
            topic: kafka_msg.topic,
            partitionid: kafka_msg.partition,
            consumer_group,
            offset: kafka_msg.offset,
            handling_state
        };
    }

}


module.exports.PersistentHandler = PersistentHandler;
