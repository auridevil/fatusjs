/**
 * Created by mox on 21/03/16.
 */
/**
 * Created by mox on 21/03/16.
 */

'use strict';

const MODULE_NAME = 'FatusWorker';
const shortid = require('shortid');
const EventEmitter = require('events');
const util = require('util');

class FatusWorker extends EventEmitter{

    constructor (fatusQueue){
        super();
        this.name = shortid.generate();
        this.fatus = fatusQueue;
        console.log(MODULE_NAME + ': just created %s', this.name);
        //util.inherits(this(),EventEmitter);
    }

}

module.exports = FatusWorker;

//export default FatusWorker;