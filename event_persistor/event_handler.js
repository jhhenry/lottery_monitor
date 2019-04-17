const event_persist = require('./event_persist');
const path = require('path');
const LOGGER = require('./logUtils')({prefix: `<${path.basename(__filename)}> `, style: "bgBlue"});

class PersistentHandler {

    constructor(consumer_group, host, port, user, pwd, database) {
        this.consumer_group = consumer_group;
        this.pool = event_persist.createPool(host, port, user, pwd, database);
    }

    async handleEvent(e) {
        if (!e) {
            return;
        }
        //log(`getConnection: ${this.pool_p.getConnection}`)
        const event = JSON.parse(e.value);
        //log(`handling event: ${event.event}`);
        try {
            if (event.event == 'RedeemedLotttery') {
                //log('handling the RedeemedLotttery: ', event.transactionHash);
                const p = await this.pool;
                const conn = await p.getConnection();
                //log(`conn: ${conn}`);
                const eventValues = {
                    blockNumber: event.blockNumber,
                    event_type: event.event,
                    event_capture_time: event.capture_time,
                    txn: event.transactionHash,
                    event_info: JSON.stringify(event)
                };
                const redeemedValues = {
                    beneficiary: event.returnValues.winner,
                    lottery_sig: event.lottery_sig,
                    payer: event.returnValues.issuer,
                    rs1: event.returnValues.rs1,
                    rs2: event.returnValues.rs2,
                    sender: event.returnValues.sender
                };
                try {
                    // stored the kafka message to the event_received table and
                    // set "processing" to the "handling_state" field.
                    // if the table already hold the message and the state is "stored"
                    // then skip  the following "recordRedeemedEvent" operation.
                    //await storeEventReceived
                    const kafka_event = {
                        topic: e.topic,
                        partitionid: e.partition,
                        consumer_group: this.consumer_group,
                        offset: e.offset,
                        handling_state: "processing"
                    };
                    const [id, handling_state] = await event_persist.recordKafkaEventReceived(conn, kafka_event);
                    if (handling_state == "stored") {
                        LOGGER(`recordKafkaEventReceived result: , ${[id, handling_state]}`);
                        return;
                    } else {
                        kafka_event.id = id;
                        return await event_persist.recordRedeemedEvent(conn, eventValues, redeemedValues, kafka_event);
                    }               
                    // no matter what happened before, send a message to a separate kafka topic "redeemed_persisted"
                    // to indicate that a new event has been persisted.
                } catch(err) {
                    if (err.code && err.code == 'ER_DUP_ENTRY') {
                        // TODO: send to a specific topic of the kafka
                        LOGGER('got dup error');
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

}

module.exports.PersistentHandler = PersistentHandler;
