/**
 * Created by mox on 21/03/16.
 *
 * Fatus worker
 *
 */


'use strict';

const MODULE_NAME = 'FatusWorker';
const EMPTY_QUEUE_RETRY_TIME = 4000; // millisec
const MAX_WORKER_ATTEMPT = 2;
const shortid = require('shortid');
const EventEmitter = require('events');
const util = require('util');
const path = require('path');
const assert = require('assert');
const retry = require('retry');
const async = require('async');

class FatusWorker extends EventEmitter{

    /**
     * constructor
     * @param fatusQueue the reference to the fatus object queue
     */
    constructor (fatusQueue){
        super();
        assert(typeof fatusQueue,'object','Missing fatusQueue in the fatus worker constructor');
        this.name = shortid.generate();
        this.fatus = fatusQueue
        console.log(MODULE_NAME + '%s: just created', this.name);
        this.emit('load',this.name);
    }

    /**
     * run the worker and execute the code
     * @param attempt number (if 0 don't pass) - expect an INT
     */
    run(attempt){
        attempt = (attempt ? attempt : 0) +1;
        var th = this;
        console.log(MODULE_NAME + '%s: running worker run()',this.name);
        this.fatus.getQueueSize(
            function onSize(err,data){
                if(data && data>0) {
                    console.log(MODULE_NAME + '%s: queue is alive, open SingleRUN, by %s',th.name,EMPTY_QUEUE_RETRY_TIME);
                    th.single();
                }else if (err){
                    console.error(err);
                }else{
                    if(attempt<MAX_WORKER_ATTEMPT) {
                        console.log(MODULE_NAME + '%s: queue is empty, retry in %s',th.name,EMPTY_QUEUE_RETRY_TIME);
                        setTimeout(function(){ th.run(attempt) },EMPTY_QUEUE_RETRY_TIME)
                    }else{
                        console.log(MODULE_NAME + '%s: queue is empty, KILLING WORKER',th.name);
                    }
                }
            }
        );
    }

    /**
     * execute a single peek-execute-pop cycle
     */
    single(){
        let th = this;
        let msgObj;
        try {
            async.waterfall([
                    // get work from queue
                    function top(wfcallback) {
                        console.log(MODULE_NAME + '%s: fetch from queue ', th.name);
                        //th.fetchNewJob(th, wfcallback);
                        th.fetchNewJob(th,wfcallback);
                    },
                    // execute instruction in message
                    function msgExecute(msg, wfcallback) {
                        assert(typeof msg, 'object', 'msg from queue is null');
                        msgObj = msg[0];
                        if(msgObj) {
                            assert(typeof msgObj.messageText, 'object', 'msg from queue is invalid');
                            console.log(MODULE_NAME + '%s: executing from queue ', th.name);
                            th.executeJob(msgObj, th, wfcallback);
                        }else{
                            wfcallback(new Error('queue is empty'),null);
                        }
                    },
                    // pop message if ok
                    function postExecute(res, wfcallback){
                        th.popMessageFT(msgObj,th,wfcallback);
                    }
                ],
                // update if error, else ok
                function _onFinish(err,val){
                    if(err && typeof err == 'error' && msgObj){
                        // async error, update message
                        th.updateMsgOnError(msgObj, err, th);
                    }
                    th.emit('runcomplete');
                    th.run();
                });
        }catch(err){
            // sync error, update message
            if(msgObj) {
                th.updateMsgOnError(msgObj, err, th);
            }s
        }
    }

    /**
     * update the message after an error
     * @param msgObj the object
     * @param err the error that caused the code to arrive here
     * @param th the reference to the worker (pointer)
     */
    updateMsgOnError(msgObj, err, th) {
        msgObj.messageText.retry = msgObj.messageText.retry ? (msgObj.messageText.retry + 1) : 1;
        if (!msgObj.messageText.failArray) {
            msgObj.messageText.failArray = [];
        }
        msgObj.messageText.failArray.push(util.inspect(err));
        th.updateMessageFT(msgObj, th, wfcallback);
    }

    /**
     * run the job at low level - used for overriding
     * @param msg
     * @param th
     * @param callback
     */
    executeJob(msg, th, callback) {
        //th.emit('msg-loaded', msg.messageText);
        th.execute(msg.messageText.module, msg.messageText.function, msg.messageText.payload, callback);
    }

    /**
     * execute the job at the lowest level
     * @param moduleName the module name to be executed (with path)
     * @param funct the function to be executed (the name of)
     * @param payload the payload to pass at the function invoked
     * @param onComplete callback
     */
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
            console.error(error);
        }
    }

    /**
     * get a new job from the queue
     * @param th
     * @param wfcallback
     */
    fetchNewJob(th, wfcallback) {
        th.fatus.getQueueTop(wfcallback);
    }

    /**
     * pop a message from the queue, while completed,with fault tollerance
     * @param msg the message to pop
     * @param th the reference to the worker
     * @param callback classic err/res callback
     */
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
                    if (ftOperation.retry(err)){
                        return;
                    }
                    callback(err ? ftOperation.mainError() : null, res);
                })
            });
    }

    /**update a messase in the queue, with fault tollerance
     * run a update
     * @param msg
     * @param th
     * @param callback
     */
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


    /**
     * return a moduble by name/path
     * @param moduleName the pathname
     * @returns a modole itself
     */
    getModule(moduleName){
        assert(typeof moduleName,'string','moduleName must be a string');
        return require(this._getPath(moduleName));
    }

    /**
     * util method to get the path normalized
     * @param inputPath
     * @returns {string}
     */
    _getPath(inputPath){
        assert(typeof inputPath,'string','inputPath must be a string');
        var base = global.__base || __dirname + '/';
        let normalizedPath = path.normalize(base + inputPath);
        return normalizedPath;
    }







}

module.exports = FatusWorker;

