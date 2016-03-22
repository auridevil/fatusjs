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
const assert = require('assert');
const retry = require('retry');

class FatusWorker extends EventEmitter{

    constructor (fatusQueue){
        super();
        assert(typeof fatusQueue,'object','Missing fatusQueue in the fatus worker constructor');
        this.name = shortid.generate();
        this.fatus = fatusQueue
        console.log(MODULE_NAME + ': just created %s', this.name);
        this.emit('load',this.name);
    }


    getPath(inputPath){
        assert(typeof inputPath,'string','inputPath must be a string');
        let normalizedPath = path.normalize(global.__base + inputPath);
        return normalizedPath;
    }

    getModule(moduleName){
        assert(typeof moduleName,'string','moduleName must be a string');
        return require(this.getPath(moduleName));
    }

    execute(moduleName,funct,payload,onComplete){
        assert(typeof moduleName,'string','moduleName must be a string');
        assert(typeof funct,'string','funct must be a string');
        assert(typeof payload,'object','payload must be a string');
        assert(typeof onComplete,'function','payload must be a string');
        try{
            let module = this.getModule(moduleName);
            module[funct](payload,this,onComplete);
        }catch(error){
            // sync errors
            // manage errors
        }
    }

    run(){
        let th = this;
        let msgObj;
        try {
            async.waterfall([
                // get work from queue
                function top(wfcallback) {
                    th.fetchNewJob(th, wfcallback);
                },
                // execute instruction in message
                function msgExecute(msg, wfcallback) {
                    msgObj = msg;
                    assert(typeof msg, 'object', 'msg from queue is null');
                    assert(typeof msg.messageText, 'object', 'msg from queue is invalid');
                    th.executeJob(msg, msgObj, th, wfcallback);
                },
                // pop message if ok
                function postExecute(res, wfcallback){
                    th.popMessageFT(msgObj,th,wfcallback);
                }
            ],
                // update if error, else ok
            function _onFinish(err,val){
                if(err && typeof err == 'error'){
                    // async error, update message
                    th.updateMsgOnError(msgObj, err, th);
                }
                th.emit('runcomplete');
            });
        }catch(error){
            // sync error, update message
            th.updateMsgOnError(msgObj, err, th);
        }
    }

    updateMsgOnError(msgObj, err, th) {
        msgObj.messageText.retry = msgObj.messageText.retry ? (msgObj.messageText.retry + 1) : 1;
        if (!msgObj.messageText.failArray) {
            msgObj.messageText.failArray = [];
        }
        msgObj.messageText.failArray.push(util.inspect(err));
        th.updateMessageFT(msgObj, th, wfcallback);
    }

    executeJob(msg, th, callback) {
        th.emit('msg-loaded', msg.messageText);
        th.execute(msg.messageText.module, msg.messageText.function, JSON.parse(msg.messageText.payload), callback);
    }

    fetchNewJob(th, wfcallback) {
        th.fatus.getQueueTop(wfcallback);
    }

    popMessageFT(msg, th, callback){
        th.emit('pop',msg.messageText);
        var ftOperation = retry.operation({
            retries: 10,                    // number of retry times
            factor: 1,                      // moltiplication factor for every rerty
            minTimeout: 1 * 1000,           // minimum timeout allowed
            maxTimeout: 1 * 1000            // maximum timeout allowed
        });
        ftOperation.attempt(
            function (currentAttempt){
                th.fatus.popMsg(msg,function(err,res){
                    if (operation.retry(err)){
                        return;
                    }
                    callback(err ? operation.mainError() : null, res);
                })
            });
    }

    updateMessageFT(msg, th, callback){
        var ftOperation = retry.operation({
            retries: 10,                    // number of retry times
            factor: 1,                      // moltiplication factor for every rerty
            minTimeout: 1 * 1000,           // minimum timeout allowed
            maxTimeout: 1 * 1000            // maximum timeout allowed
        });
        ftOperation.attempt(
            function (currentAttempt){
                th.fatus.updateMessage(msg,function(err,res){
                    if (operation.retry(err)){
                        return;
                    }
                    callback(err ? operation.mainError() : null, res);
                })
            });
    }




}

module.exports = FatusWorker;

