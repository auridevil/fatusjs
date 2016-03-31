/**
 * Created by Aureliano on 23/03/16.
 */

"use strict";

const MODULE_NAME = 'MessageJob';
const EventEmitter = require('events');
const util = require('util');
const path = require('path');
const assert = require('assert');
const retry = require('retry');
const async = require('async');
const moment = require('moment');
const funcster = require('funcster');

/** 
 * This obects is a job (new or from the azure queue)
 */
class MessageJob extends EventEmitter {

    /**
     * create a new job obj
     * @param msg if passed init the job with a previous saved msg, empty for new
     */
    constructor(msg) {
        super();
        if (msg && msg.messageText && msg.messageText.isSimple) {
            this.setSimpleJob(msg.messageText.module, msg.messageText.function, msg.messageText.payload, msg.messageText.fail, msg.messageText.failArray);
            this.originalMsg = msg;
            this.id = msg.messageText.id;
        } else if (msg && msg.messageText && msg.messageText.isMulti) {
            this.setMultiJobFromMsg(msg.messageText);
            this.originalMsg = msg;
            this.id = msg.messageText.id;
        } else {
            // NOTHING TO DO, NEW OBJECT
        }
    }

    /**
     * init the job as a singlestep job
     * @param moduleName the module name to be required for the job
     * @param functionName the name of the function to be executed (without '()'), as a string
     * @param payload the payload obj to be passed to the function
     * @param fail the number of previous failure, or 0
     * @param failArray the array of previous failure, or []
     * @return a pointer to the job itself
     */
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

    /**
     * init the job as a multistep job
     * @return a pointer to the job itself
     */
    setMultiJob() {
        assert.ok(!this.isSimple, 'already a simple object');
        this.isMulti = true;
        this.fail = 0;
        this.failArray = [];
        this.stepDone = [];
        this.stepNext = [];
        return this;
    }

    /**
     * add a step to the multistep job, paramobj should be inited only for first step
     * @param module the module name to be required
     * @param functionName the name of the function to be invoked by the step (without '()' ), as a string
     * @param paramObj the object to be setted as a parameter to the step invokation, should be setted only to first
     * @return a pointer to the job itself
     */
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

    /**
     * init the job from the messageText coming from azure
     * @return a pointer to the job itself
     */
    setMultiJobFromMsg(msgText){
        this.setMultiJob();
        this.fail = msgText.fail || 0;
        this.failArray = msgText.failArray || [];
        this.stepDone = msgText.stepDone;
        this.stepNext = msgText.stepNext;
        return this;
    }

    /** 
     * get the inner message (excluded from the azure queue infos)
     * @return msg the inner message representing the job itself
     */
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
        outMsg.id = this.id;
        return outMsg;
    }

    /**
     * get the complete message object from azure queues
     * @return the inner message object
     */
    getCompleteMsg(){
        var msg = this.originalMsg || {};
        msg.messageText = this.getMsg();
        return msg;
    }

    /**
     * return the inner messageId of the job msg
     * @return the inner messageId
     */
    getId(){
        var msg = this.originalMsg || {};
        return msg.messageId;
    }

    /**
     * method to be called from outside for executing job
     * @param worker is the *ptr to the worker
     * @param onComplete is the function(e,r) to be called in the end
     */
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

    /**
     * execute a simple job
     * @param worker is the *ptr to the worker
     * @onComplete is the function(e,r) to be called in the end
     */
    executeSimple(worker,onComplete) {
        let th = this;
        try {
            // execute module.function() by reflection
            let module = this.getModule(this.module);
            console.log(MODULE_NAME + ': executing %s.%s', this.module, this.function);
            module[this.function](
                this.payload, 
                worker, 
                function onOk(err,res){
                   if(err){
                       th.fails(err);
                   }
                   onComplete(err,res);
                });
            
        } catch (error) {
            // sync errors
            console.error(error);
            th.fails(err);
            onComplete(error,null);
        }
    }

    /**
     * execute a multistep job
     * @param worker is the *ptr to the worker
     * @onComplete is the function(e,r) to be called in the end
     */
    executeMulti(worker,onComplete) {
        let th = this;
        try{
            async.whilst(
                
                // first function is the looping condition
                function isMoreToProcess(){
                    var cond1 = th.stepNext.length>0;  // there is a element to execute
                    var cond2 = !(th.stepNext.suspended); // the step is not suspended
                    return cond1 && cond2;
                },
                
                // second function is executed by the loop
                function process(callback){
                    th.steplevel = th.steplevel + 1;
                    let step = th.stepNext[0];
                    th.processStep(step,worker,callback);
                },
                
                // third function is the finishing function
                function onFinish(err,res){
                    if(err){
                        // th.fails(err); this is redundant, error is already traced in step processing
                        worker.updateMsg(th.getCompleteMsg(),onComplete);
                    }else{
                        console.log(MODULE_NAME + ': exiting step mode');
                        onComplete(null,{success:true});
                    }
                }
            );
        }catch( error){
            // syncronous error, should not happens
            console.error(error);
            onComplete(error,null);
        }
    }

    /**
     * process a single step from a multistep job
     * no update (in theory) are performed nor reservation
     * @param step the step to be executed
     * @param worker the *ptr to the worker
     * @param callback the function(e,r) to be executed in the end
     */
    processStep(step,worker,callback){
        let th = this;
        try{
            async.waterfall([

                // reserve the object
                function reserveAndInit(wfcallback){
                    // HERE IS ALREADY RESERVED BY WORKER 
                    // seems that too many access to queue cause the update to fail 
                    //th.reserve(worker,wfcallback);
                    wfcallback(null,null);
                },

                // execute
                function exe(res,wfcallback){
                    let module = th.getModule(step.moduleName);
                    console.log(MODULE_NAME + ': executing %s.%s -  %s', step.moduleName, step.function,th.originalMsg ? th.originalMsg.messageId:'ND');
                    // invoke the module.function() by reflections
                    if(step.payload.skipWorker) {
                        // in this case the function is ignoring to be behind fatus (poor function)
                        module[step.function](step.payload, wfcallback);
                    }else{
                        // in this case the function executing is fatus aware and need the worker
                        module[step.function](step.payload, worker, wfcallback);
                    }
                },

                // update and unlock
                function update(paramObj,wfcallback){
                    th.popStep();
                    th.updateStepPayload(paramObj);
                    //worker.updateMsg(th.getCompleteMsg(),wfcallback); // SKIP UPDATE
                    wfcallback();
                },

                // complete step
                // keeping for the future
                function complete(wfcallback){
                    wfcallback();
                }
            ],
                            
            // sum up and complete
            function onComplete(err,res){
                if(err){
                    // update object on fail
                    th.fails(err,step);
                    //worker.updateMsg(th.getCompleteMsg(),callback); moving update in the parent caller -> execute multi
                }
                // leave error to be managed by upper object
                callback(err,res)
            })
        }catch(err){
            // if a syntax/syncronous error happens, the job is in fail state
            th.fails(err,step);
            //worker.updateMsg(th.getCompleteMsg(),callback); moving update in the parent caller -> execute multi
            callback(err,null);
        }
    }
    
    
    
    /**
     * pop out the step from the nextStep array
     * and move in the done array
     * (no return and no callback)
     */
    popStep(){
        assert.ok(!this.isSimple,'the job is a simple obj');
        assert.equal(this.stepNext.length>0,true,'the are no current step');
        var step = this.stepNext[0];
        this.stepNext.splice(0,1); // remove the 0 indexed item
        this.stepDone.push(step);
    }
    
    /**
     * update the payload for the next step and 
     * execute post running function if any (by creator)
     * (warning, the function have to be deserialized by funcster)
     * this function have no return value nor a callback function
     * @param the payload of the current step
     */
    updateStepPayload(payload){
        assert.ok(!this.isSimple,'the job is a simple obj');
        if(this.stepNext.length>0) {
            if(payload.postRunFunction){
                payload = funcster.deepDeserialize(payload);
                try {
                    payload = payload.postRunFunction(payload);
                }catch(err){
                    console.error(err);
                }
            }
            this.stepNext[0].payload = payload;
        }
    }

    /**
     * trace a fail by the job
     * @param err the error that caused the fail
     * @param step the step - if any - that caused the fail
     */
    fails(err,step){
        assert.equal(typeof err,'object','Error is null');
        this.fail = this.fail +1;
        this.reserved = false;
        try {
            this.failArray.push(util.inspect(err));
            // limit the fail array to 20 element
            if(this.failArray.length>20){
                let diff = this.failArray.length - 20;
                this.failArray.splice(0,diff);
            }
        }catch(inspecterr){
            console.error(inspecterr);
        }

        if(step){
            step.fail = step.fail+1;
            if(step.payload && step.payload.postFailFunction){
                try {
                    step.payload = funcster.deepDeserialize(payload);
                    step.payload = payload.postFailFunction(payload,step,err);
                }catch(err){
                    console.error(err);
                }

            }
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
     * unreserve object
     * @param save
     * @param onDone
     */
    unreserve(save,onDone){
        this.reserved = false;
        if(save){
            var msg = this.getCompleteMsg();
            if(!msg || !msg.messageId){
                console.log(MODULE_NAME + ': CANNOT UNRESERVE EMPTY OBJECT ' + util.inspect(msg));
                onDone(null,null);
            }else {
                worker.updateMsg(this.getCompleteMsg(), onDone);
            }
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
    
    /**
     * util method for serializing function to be saved in the job object msg
     * @param the obj to be serialized
     * @return the obj version serialized
     */
    static serialize(obj){
        return funcster.deepSerialize(obj);
    }

}

/** Exports */
module.exports = MessageJob;
