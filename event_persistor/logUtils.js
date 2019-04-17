const chalk = require('chalk');
function getLogger(options) {
    if (process.env.NODE_ENV == "test") {
        return function (message) {
            let m = message;
            if (options.style) {
                m = chalk[options.style](m);
            }
            if (options.prefix) {
                m = chalk.blue(options.prefix) + m;
            }
           
            console.log(m)
        }
    } else {
        return function () {

        }
    }
}

module.exports = getLogger;

//test code
// const LOG = getLogger({prefix: 'test'});
// LOG('hello');
