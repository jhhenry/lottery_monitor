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
                        //console.log(`txn: ${e.transactionHash}`)
                        sendToKafka(web3, producer, topic, e, "key", Date.now());
                    });
                }
            });
        }
        
        lotteryC.events.RedeemedLotttery().on(
            "data", e => {
                sendToKafka(web3, producer, topic,  e, "key", Date.now());
        });
    });
}

async function sendToKafka(web3, producer, topic, e, key, timestamp) {
    e.capture_time = (new Date()).toISOString().substr(0, 22);
    const lottery_sig = await getLotterySignature(web3, e);
    e.lottery_sig = lottery_sig;
    
    const event = JSON.stringify(e);
    // console.log(`timestamp: ${timestamp}`);
    producer.produce(topic, null, Buffer.from(event), key, timestamp);
}

async function getLotterySignature(web3, e) {
    const txn = await web3.eth.getTransaction(e.transactionHash);
    const params = web3.eth.abi.decodeParameters(['bytes', 'bytes', 'bytes'], '0x' + txn.input.substr(10));
    return params[1];    
}

module.exports = run;
