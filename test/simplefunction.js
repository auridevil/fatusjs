/**
 * Created by aureliano on 23/03/16.
 */


var util = require('util');


function invoke(payload,worker,onComplete){
    "use strict";

    console.log('Hello i am the happy function');
    console.log(util.inspect(payload));
    onComplete(null,'TODOBIEN');
}

exports.invoke = invoke;