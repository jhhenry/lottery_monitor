const event_persist = require('./event_persist');
const log = console.log;

class PersistentHandler {

    constructor(host, port, user, pwd, database) {
        this.pool = event_persist.createPool(host, port, user, pwd, database);
    }

    async handleEvent(event) {
        if (!event) {
            return;
        }
        //log(`getConnection: ${this.pool_p.getConnection}`)
        log(`handling event: ${event.event}`);
        try {
            if (event.event == 'RedeemedLotttery') {
                const p = await this.pool;
                const conn = await p.getConnection();
                //log(`conn: ${conn}`);
                log('handling the RedeemedLotttery: ', event.transactionHash);
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
                    return await event_persist.recordRedeemedEvent(conn, eventValues, redeemedValues);
                } catch(err) {
                    if (err.code && err.code == 'ER_DUP_ENTRY') {
                        // TODO: send to a specific topic of the kafka
                        console.log('got dup error');
                    } else {
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
