const Web3 = require('web3');
const KafkaClient = require('./kafka_client');

function run(eth_node, ws_origin, contract_address, abi, since, kafka_brokers, topic) {
    //eval()
    const provider = new Web3.providers.WebsocketProvider(eth_node, {headers: {
    Origin: ws_origin
    }});
    provider.on('error', e => console.error('WS Error', e));
    provider.on('end', e => console.error('WS End', e));

    const web3 = new Web3(provider);
    //connect to ethereum's contract using web3
    const lotteryC = new web3.eth.Contract(abi, contract_address);
    console.log("start monitoring RedeemedLottery events");
    // start monitoring the specific event of the contract
    // subscribe the event
    KafkaClient.getProducer(kafka_brokers).then(r => {
        const producer = r;
        if(!isNaN(since)) {
            lotteryC.getPastEvents("RedeemedLotttery", {fromBlock: since}).then(events => {
                if(events.forEach) {
                    events.forEach(e => {
                        sendToKafka(producer, topic, e, "key", Date.now());
                    });
                }
            });
        }
        
        lotteryC.events.RedeemedLotttery().on(
            "data", e => {
                sendToKafka(producer, topic,  e, "key", Date.now());
        });
    });
}

function sendToKafka(producer, topic, e, key, timestamp) {
    const event = JSON.stringify(e);
    console.log(`timestamp: ${timestamp}`);
    producer.produce(topic, null, Buffer.from(event), key, timestamp);
}

module.exports = run;
