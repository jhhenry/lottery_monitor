const chalk = require('chalk');
const _ = require('lodash');
function getLogger(options) {
    if (process.env.NODE_ENV == "test") {
        return function (...messages) {
            let msg = messages.map(m => {
                m = constructMessage(m);
                if (options.style) {
                    m = chalk[options.style](m);
                }
               
               return m;
            }).reduce((sum, v) => {
                sum += v;
                return sum;
            });
            if (options.prefix) {
                msg = chalk.blue(options.prefix) + msg;
            }
           console.log(msg)
        }
    } else {
        return function () {

        }
    }
}

function constructMessage(message, level = 1) {
    if (_.isNil(message)) {
        return message;
    }
    if (_.isNumber(message)) {
        return message;
    }
    if (_.isBoolean(message)) {
        return message;
    }
    if (_.isString(message)) {
        return level == 1 ? message : `"${message}"`;
    }
    if (_.isArray(message)) {
        let arrayM = 
        _.join(
            _.map(message, function(m) {
                return constructMessage(m, level + 1);
        }), ', ');
        return `[${arrayM}]`;
    }

    if(_.isMap(message)) {
        const arr = [];
        message.forEach( 
            (value, key) => {
                arr.push((constructMessage(key) + " => " +constructMessage(value)));
            }
        );
        const mapM = _.join(arr, ', \n');
        return `Map: {\n${mapM}\n}`;
    }

    if (_.isDate(message)) {
        return message.toLocaleDateString();
    }
   
    const propStrings = [];
    const tabStr = _.join(_.times(level, _.constant('  ')),"");
    _.forIn(message, function(value, key) {
        propStrings.push(`${tabStr}${key}: ${constructMessage(value, level + 1)}`);
    })
    return "{\n" + _.join(propStrings, ",\n") + `\n${ _.join(_.times(level - 1, _.constant('  ')),"")}}`;
}

module.exports = getLogger;

//test code
// const LOG = getLogger({prefix: 'test'});
// LOG('hello');
