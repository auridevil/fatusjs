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
const path = require('path');

class FatusWorker extends EventEmitter{

    constructor (fatusQueue){
        super();
        this.name = shortid.generate();
        this.fatus = fatusQueue
        console.log(MODULE_NAME + ': just created %s', this.name);
        //util.inherits(this(),EventEmitter);
    }


    getPath(inputPath){
        let normalizedPath = path.normalize(global.__base + inputPath);
        return normalizedPath;
    }

    getModule(moduleName){
        return require(this.getPath(moduleName));
    }

    execute(moduleName,funct,payload,onComplete){
        try{
            let module = getModule(moduleName);
            module[funct](payload,this,onComplete);
        }catch(exception){
            // manage exception
        }
    }

    run(){
        let th = this;
        async.waterfall([
            
            function top(wfcallback){
                th.fatus.getQueueTop(wfcallback);
            },

            function msgExecute(msg,wfcallback){
                th.execute(msg.messageText.module,msg.messageText.function,JSON.parse(msg.messageText.payload),wfcallback);
            }

            // continue from here
        ]
    }






}

module.exports = FatusWorker;

