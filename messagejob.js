/**
 * Created by mox on 23/03/16.
 */

"use strict";

const MODULE_NAME = 'MessageJob';
const EventEmitter = require('events');
const util = require('util');
const path = require('path');
const assert = require('assert');
const retry = require('retry');
const async = require('async');

class MessageJob extends EventEmitter {

    constructor(msg) {
        super();
        if (msg && msg.messageText && msg.messageText.isSimple) {
            this.setSimpleJob(msg.messageText.module, msg.messageText.function, msg.messageText.payload, msg.messageText.fail, msg.messageText.failArray);
        } else if (msg && msg.messageText && msg.messageText.isMulti) {
            this.setMultiJobFromMsg(msg.messageText);
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
        assert.equal(typeof paramObj, 'object', 'paramObj must be a string');
        assert.equal(this.isSimple, false, 'already a simple object');
        var step = {
            moduleName: module,
            function: functionName,
            payload: paramObj,
            fail: 0
        }
        this.stepNext.push(step);
        return this;
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
                payload: this.payload
            }
        }else if(this.isMulti){
            outMsg = {
                isMulti:this.isMulti,
                fail:this.fail,
                stepDone:this.stepDone,
                stepNext:this.stepNext
            }
        }
        return outMsg;
    }

    execute(worker,onComplete){
        if(this.isSimple) {
            this.executeSimple(worker,onComplete);
        }else if (this.isMulti){
            //this.executeMulti(onComplete);
        }
    }

    executeSimple(worker,onComplete) {
        try {
            let module = this.getModule(this.module);
            module[this.function](this.payload, this, onComplete);
        } catch (error) {
            // sync errors
            console.error(error);
            onComplete(error,null);
        }
    }

    fail(err){
        this.fail = this.fail +1;
        try {
            this.failArray.push(util.inspect(err));
        }catch(inspecterr){
            console.error(inspecterr);
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
