const event_persist = require('./event_persist');
class PersistentHandler {
    constructor(host, port, user, pwd, database) {
        this.pool = await event_persist.createPool(host, port, user, pwd, database);
    }

    async handleEvent(event) {
        if (event) {
            const conn = await getConnectionFromPool(this.pool);
            try {
                if (event.event === 'RedeemedLotttery') {
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
                    await event_persist.recordRedeemedEvent(conn, eventValues, redeemedValues);
                }
            } catch(err) {

            } finally {
                if (conn) {conn.release();}
            }
        }
    }

}

module.exports.PersistentHandler = PersistentHandler;
