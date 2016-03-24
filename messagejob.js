/**
 * Created by mox on 23/03/16.
 */

"use strict";

const MODULE_NAME = 'MessageJob';
const OVER_REPETITION_THRSH = 10;
const EventEmitter = require('events');
const util = require('util');
const path = require('path');
const assert = require('assert');
const retry = require('retry');
const async = require('async');
const moment = require('moment');

class MessageJob extends EventEmitter {

    constructor(msg) {
        super();
        if (msg && msg.messageText && msg.messageText.isSimple) {
            this.setSimpleJob(msg.messageText.module, msg.messageText.function, msg.messageText.payload, msg.messageText.fail, msg.messageText.failArray);
            this.originalMsg = msg;
        } else if (msg && msg.messageText && msg.messageText.isMulti) {
            this.setMultiJobFromMsg(msg.messageText);
            this.originalMsg = msg;
        } else {
            // NOTHING TO DO, NEW OBJECT
        }
    }

    setSimpleJob(moduleName, functionName, payload, fail, failArray) {
        assert.equal(typeof moduleName, 'string', 'module must be a string');
        assert.equal(typeof functionName, 'string', 'function must be a string');
        assert.equal(typeof payload, 'object', 'payload must be a object');
        assert.ok(!this.isMulti, 'already a multi object');
        this.module    = moduleName;
        this.function  = functionName;
        this.payload   = payload;
        this.fail      = fail || 0 ;
        this.failArray = failArray || [];
        this.isSimple  = true;
        return this;
    }

    setMultiJob() {
        assert.ok(!this.isSimple, 'already a simple object');
        this.isMulti = true;
        this.fail = 0;
        this.failArray = [];
        this.stepDone = [];
        this.stepNext = [];
        return this;
    }

    addStep(module, functionName, paramObj){
        assert.equal(typeof module, 'string', 'module must be a string');
        assert.equal(typeof functionName, 'string', 'function must be a string');
        assert.ok(!this.isSimple, 'already a simple object');
        var step = {
            moduleName: module,
            function: functionName,
            payload: paramObj,
            fail: 0
        }
        this.stepNext.push(step);
        return this;
    }

    popStep(){
        assert.ok(!this.isSimple,'the job is a simple obj');
        assert.equal(this.stepNext.length>0,true,'the are no current step');
        var step = this.stepNext[0];
        this.stepNext.splice(0,1); // remove the 0 indexed item
        this.stepDone.push(step);
    }

    updateStepPayload(payload){
        assert.ok(!this.isSimple,'the job is a simple obj');
        if(this.stepNext.length>0) {
            this.stepNext[0].payload = payload;
        }
    }

    setMultiJobFromMsg(msgText){
        this.setMultiJob();
        this.fail = msgText.fail || 0;
        this.failArray = msgText.failArray || [];
        this.stepDone = msgText.stepDone;
        this.stepNext = msgText.stepNext;
        return this;
    }

    getMsg(){
        let outMsg;
        if(this.isSimple){
            outMsg = {
                module: this.module,
                function: this.function,
                fail: this.fail,
                isSimple: this.isSimple,
                payload: this.payload,
                failArray: this.failArray
            }
        }else if(this.isMulti){
            outMsg = {
                isMulti: this.isMulti,
                fail: this.fail,
                stepDone: this.stepDone,
                stepNext: this.stepNext,
                failArray: this.failArray
            }
        }
        if(this.reserved && outMsg){
            outMsg.reserved = this.reserved;
            outMsg.dtReserve = this.dtReserve;
        }
        return outMsg;
    }

    getCompleteMsg(){
        var msg = this.originalMsg || {};
        msg.messageText = this.getMsg();
        return msg;
    }

    execute(worker,onComplete){
        if(this.isSimple) {
            this.executeSimple(worker,onComplete);
        }else if (this.isMulti){
            this.executeMulti(worker,onComplete);
        }else{
            // empty for some reason
            console.log(MODULE_NAME + ': aborted empty operation');
            onComplete(null,null);
        }

    }

    executeSimple(worker,onComplete) {
        try {
            let module = this.getModule(this.module);
            console.log(MODULE_NAME + ': executing %s.%s', this.module, this.function);
            module[this.function](this.payload, worker, onComplete);
        } catch (error) {
            // sync errors
            console.error(error);
            onComplete(error,null);
        }
    }

    executeMulti(worker,onComplete) {
        let th = this;
        try{
            async.whilst(
                function isMoreToProcess(){
                    var cond1 = th.stepNext.length>0;  // there is a element to execute
                    var cond2 = !(th.stepNext.suspended); // the step is not suspended
                    return cond1 && cond2;
                },
                function process(callback){
                    th.steplevel = th.steplevel + 1;
                    let step = th.stepNext[0];
                    th.processStep(step,worker,callback);
                },
                function onFinish(err,res){
                    if(err){
                        th.fails(err);
                        worker.updateMsg(th.getCompleteMsg(),onComplete);
                    }else{
                        console.log(MODULE_NAME + ': exiting step mode');
                        onComplete(null,{success:true});
                    }
                }
            );
        }catch( error){
            console.error(error);
            onComplete(error,null);
        }
    }

    processStep(step,worker,callback){
        let th = this;
        try{
            async.waterfall([

                // reserve the object
                function reserve(wfcallback){
                    th.reserve(worker,wfcallback);
                },

                // execute
                function exe(res,wfcallback){
                    let module = th.getModule(step.moduleName);
                    console.log(MODULE_NAME + ': executing %s.%s', step.moduleName, step.function);
                    module[step.function](step.payload, worker, wfcallback);
                },

                // update and unlock
                function update(paramObj,wfcallback){
                    th.popStep();
                    th.updateStepPayload(paramObj);
                    th.reserved = false;
                    worker.updateMsg(th.getCompleteMsg(),wfcallback);
                },

                // complete step
                function complete(res,wfcallback){
                    wfcallback();
                }
            ],
                // sum up
            function onComplete(err,res){
                if(err){
                    th.fails(err);
                    worker.updateMsg(th.getCompleteMsg(),callback);
                }else{
                    callback(null,res);
                }
            })
        }catch(err){
            th.fails(err);
            worker.updateMsg(th.getCompleteMsg(),callback);
        }
    }

    /**
     * trace a fail
     * @param err
     */
    fails(err){
        this.fail = this.fail +1;
        this.reserved = false;
        try {
            this.failArray.push(util.inspect(err));
        }catch(inspecterr){
            console.error(inspecterr);
        }
    }

    /**
     * reserve the message
     * @param worker
     * @param onDone
     */
    reserve(worker,onDone){
        this.reserved = true;
        this.dtReserve = new Date();
        this.reserver = worker.name;
        var msg = this.getCompleteMsg();
        if(!msg || !msg.messageId){
            console.log(MODULE_NAME + ': CANNOT RESERVE EMPTY OBJECT ' + util.inspect(msg));
            onDone(null,null);
        }else {
            worker.updateMsg(this.getCompleteMsg(), onDone);
        }
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

module.exports = MessageJob;
