const test = require('ava');
const event_persist = require('../event_persist');
const log = console.log;

test('acquire connection from a pool test', async t => {
    const p = await event_persist.createPool('localhost', 3306, "root", 'Hjin_5105', 'test');
    const c = await p.getConnection();
    const [rows, cols] = await c.query('select * from events');
    log(`rows: ${rows}\r\ncols: ${cols}`);
    t.true(rows.length >=0 );
    t.true(cols.length > 0);
    t.pass();
})