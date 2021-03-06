
const test = require('ava');
const persistor = require('../event_persist');
const testUtils = require('./testUtils')
test.before('set up connection and create tables', async t => {
    await testUtils.createTestDatabase(t);       
});

test.after.always("cleanup: delete databases", async t => {
    await testUtils.dropTestDatabase(t)
});

test('connection test', async t => {
    const con = t.context.con;
    let [rows, fields] = await con.query("select * from events");
    console.log(`rows: ${rows.length}\nfields: ${fields[0]}`);
    t.pass();
})

test('insert into test', async t=> {
    const con = t.context.con;
    const eventInfo = {
        blockNumber: 5,
        event_type: 'RedeemedLotttery',
        event_capture_time: (new Date()).toISOString().substr(0, 22),
        txn: '0xb2b101a14487dd45c6861d6da2f7bea2de13b45af6edb2731f748bf4f41d3b98',
        event_info: `{
            "address": "0x6dEFcB6F97E4b9765B88ebcaAF8A98f6338571f3",
            "blockNumber": 4223,
            "transactionHash": "0xb2b101a14487dd45c6861d6da2f7bea2de13b45af6edb2731f748bf4f41d3b98",
            "transactionIndex": 0,
            "blockHash": "0x2d841ffd9e1ecd8a95a144c5914ab93653f5ce71905bb040604ca7fd245d5f20",
            "logIndex": 2,
            "removed": false,
            "id": "log_0x692da4314736bc80ca9c51c27040e95f8686e13d64fb8a97a4a408294eca62c0",
            "returnValues": {
                "0": "0x01143936633434376436383436346534303535636330c659068887db7414ef1a704b2159cfb75ba6396140ca8ed45ce07fea99a05c6fbd6d6abfba84efe632dd6e93ceef88a7f8333bfd00000166af0bd869f1a0abc06e0e603c6f3130ec27c58057b7d7b11d00000000000000000000000000000000000000000000000000000000000000320301",
                "1": "0x3939313439356561356562373062343839386638",
                "2": "0x3936633434376436383436346534303535636330",
                "3": "50",
                "4": "0xf1A0aBC06e0e603C6F3130eC27C58057b7D7B11D",
                "5": "0x7c8c7A481a3dAc2431745Ce9b18B3BB8b6C526e7",
                "6": "0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD",
                "7": "0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD",
                "lottery": "0x01143936633434376436383436346534303535636330c659068887db7414ef1a704b2159cfb75ba6396140ca8ed45ce07fea99a05c6fbd6d6abfba84efe632dd6e93ceef88a7f8333bfd00000166af0bd869f1a0abc06e0e603c6f3130ec27c58057b7d7b11d00000000000000000000000000000000000000000000000000000000000000320301",
                "rs1": "0x3939313439356561356562373062343839386638",
                "rs2": "0x3936633434376436383436346534303535636330",
                "faceValue": "50",
                "token_address": "0xf1A0aBC06e0e603C6F3130eC27C58057b7D7B11D",
                "issuer": "0x7c8c7A481a3dAc2431745Ce9b18B3BB8b6C526e7",
                "winner": "0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD",
                "sender": "0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD"
            },
            "event": "RedeemedLotttery",
            "signature": "0x225668dc003423e36bd8d7793625f5e7b40fd2e20a878f131af633c1d6cc88a8",
            "raw": {
                "data": "0x000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000001c000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000032000000000000000000000000f1a0abc06e0e603c6f3130ec27c58057b7d7b11d0000000000000000000000007c8c7a481a3dac2431745ce9b18b3bb8b6c526e7000000000000000000000000bd6d6abfba84efe632dd6e93ceef88a7f8333bfd000000000000000000000000bd6d6abfba84efe632dd6e93ceef88a7f8333bfd000000000000000000000000000000000000000000000000000000000000008801143936633434376436383436346534303535636330c659068887db7414ef1a704b2159cfb75ba6396140ca8ed45ce07fea99a05c6fbd6d6abfba84efe632dd6e93ceef88a7f8333bfd00000166af0bd869f1a0abc06e0e603c6f3130ec27c58057b7d7b11d000000000000000000000000000000000000000000000000000000000000003203010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014393931343935656135656237306234383938663800000000000000000000000000000000000000000000000000000000000000000000000000000000000000143936633434376436383436346534303535636330000000000000000000000000",
                "topics": [
                    "0x225668dc003423e36bd8d7793625f5e7b40fd2e20a878f131af633c1d6cc88a8"
                ]
            },
            "lottery_sig": "0x249228fe824c2d1429e8ea640cb5a6d20bae05306fe3e5d5f937a4cd0df0a1084a49d1d20ce6a12d270e8a507a3906d22df17f1af603dbfa07883747d83cf2251b"
        }`
    }
    const redeemedInfo = {
        beneficiary: '0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD',
        lottery_sig: '0x249228fe824c2d1429e8ea640cb5a6d20bae05306fe3e5d5f937a4cd0df0a1084a49d1d20ce6a12d270e8a507a3906d22df17f1af603dbfa07883747d83cf2251b',
        payer: '0x7c8c7A481a3dAc2431745Ce9b18B3BB8b6C526e7',
        rs1: '0x11111111',
        rs2: '0x22222222',
        sender: '0xbD6d6abFBA84Efe632dd6E93CEef88a7F8333bfD'
    };
    await persistor.recordRedeemedEvent(con, eventInfo, redeemedInfo);

    let [rows, fields] = await con.query("select * from events");
    //console.log(fields.length);
    t.is(rows.length, 1);
    t.is(fields.length, 6);
    const r = rows[0];
    t.is(r.id, 1)
    t.is(r.txn, eventInfo.txn);

    let [rows2, fields2] = await con.query("select * from redeemedInfo");
    t.is(rows2.length, 1);
    t.is(fields2.length, Object.keys(redeemedInfo).length);
    redeemedInfo.id = r.id;
    Object.keys(redeemedInfo).forEach(e => {
        t.is(rows2[0][e], redeemedInfo[e]);
        //console.log(`${e}: ${rows2[0][e]}`)
    })
    t.pass();
})