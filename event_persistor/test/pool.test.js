const test = require('ava');
const event_persist = require('../event_persist');


test('acquire connection from a pool test', async t => {
    const p = await event_persist.createPool('localhost', 3306, "root", 'Hjin_5105', 'test');
    //log(p);
    const c = await event_persist.getConnectionFromPool(p);
    const [rows, cols] = await c.query('select * from events');
    t.true(rows.length >=0 );
    t.true(cols.length > 0);
    t.pass();
})