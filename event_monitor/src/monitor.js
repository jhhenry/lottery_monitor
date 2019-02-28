const Web3 = require('web3');
const Kafka = require('node-rdkafka');

function run(eth_node, ws_origin, contract_address, abi, since, kafka_brokers) {
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
    lotteryC.events.RedeemedLotttery().on(
        "data", e => {
            console.log(e.returnValues);
    });
}

module.exports = run;
