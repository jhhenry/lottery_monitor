#!/usr/bin/env node

const meow = require('meow');
const log = require('./logUtils');
const monitor = require('./monitor');
const fs = require('fs');

const logBlue = log.logBlue("index.js");
const logCyan = log.logCyan("index.js");
const default_abi = fs.readFileSync("src/abi.json");
try {
    const cli = constructCLIHelper();
    const flags = cli.flags;
    logBlue(`accepted command options: \r\naddress: ${flags.address}, abi: ${flags.abi}, since: ${flags.since}, kafka: ${flags.kafka}, wsorigin: ${flags.wsorigin}`);
    const eth_info = flags.eth;
    logBlue(`eth_info: ${eth_info}`);
    const abi = JSON.parse(flags.abi ? flags.abi: default_abi);
    logBlue(`abi: ${abi}`);
    monitor(eth_info, flags.wsorigin ? flags.wsorigin: "http://localhost", flags.address, abi, flags.since, flags.kafka);
} catch(e) {
    console.error(e);
}


function constructCLIHelper()
{
    logCyan(`ready to construct cli object(meow)`);
    const helpText = `
        Run an app that monitors the specified lottery contract's events and send all events to kafka.
        Options:
            --eth     -e ether node the app will connect to. should be written as an valid URL
                        For example: ws://192.168.56.123:8566
            --wsorigin  -wo the Origin Header used when connecting to a ether node using websocket. By default, it is  http://localhost
            --address -a contract address 
            --abi -i     contract abi
            --since -s   block number since which the events are captured
            --kafka -k   kafka brokers list
    `;
    const options = {
        flags: {
            eth: {
                type: 'string',
                alias: 'e'
            },
            address: {
                type: 'string',
                alias: 'a'
            },
            abi: {
                type: 'string',
                alias: 'i'
            },
            since: {
                type: 'string',
                alias: 's'
            },
            kafka: {
                type: 'string',
                alias: 'k'
            }
        }
    }
    
    return meow(helpText, options);
}