/**
 * Created by aureliano on 23/03/16.
 */


var util = require('util');
const shortid = require('shortid');
const c = require('colors');


function invoke(payload,worker,onComplete){
    "use strict";

    console.log('Hello i am the happy function');
    console.log(util.inspect(payload));
    onComplete(null,'TODOBIEN');
}


function subsInvoke(payload,worker,onComplete){
    "use strict";

    payload.dataInfo++;
    if(!payload.shortid){
        payload.shortid = shortid.generate();
    }
    //console.log(c.red('TEST subinvoke is passing at %s',payload.dataInfo));
    //console.log(c.red('TEST subinvoke is static at %s',payload.static));
    //console.log(c.red('TEST subinvoke is shortid at %s',payload.shortid));

    console.log('T: '.red,payload.shortid, worker.name, payload.dataInfo);

    onComplete(null,payload);

}

function invokeAndFail(payload,worker,onComplete){
    "use strict";
    onComplete(new Error('BECAUSE LIFE SUCKS'),null);
}



exports.subsInvoke = subsInvoke;
exports.invoke = invoke;
exports.invokeAndFail = invokeAndFail;