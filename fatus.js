
'use strict';

/** constants */
const MODULE_NAME = 'Fatusjs'
const FATUS_QUEUE_NAME = process.env.FATUS_QUEUE_NAME || 'fatusjs-queue';
const FATUS_MAX_WORKER = process.env.FATUS_MAX_WORKER || 2;
const FATUS_EQ_RETRY_TIME = process.env.FATUS_EQ_RETRY_TIME || 4000; // millisec
const FATUS_WRK_RETRY_ATTEMP = process.env.FATUS_WRK_RETRY_ATTEMP || 2;
const FATUS_WRK_STACK_TRSHLD = process.env.FATUS_WRK_STACK_TRSHLD || 10;
const FATUS_JOB_RESERV_TIME = process.env.FATUS_JOB_RESERV_TIME || 60; // sec

/** inner refs */
const AzureQueue = require('./azurequeue');
const FatusWorker = require('./worker');
const MessageJob = require('./messagejob');

/** outher refs */
const assert = require('assert');
const EventEmitter = require('events');
const shortid = require('shortid');

/** singleton */
let singleton = Symbol();
let singletonEnforcer = Symbol();


/**
 * Lightweight azure queue job processor
 */
class Fatusjs extends EventEmitter{

    /**
     * constructor for the fatus queue
     */
    constructor(enforcer){
        // singleton enforcer pattern
        if(enforcer != singletonEnforcer){
            throw "Cannot construct singleton";
        }
        super();
        this.FatusWorker = FatusWorker; // put here to manage outside
        this.queueMgr = new AzureQueue();
        this.queueMgr.createQueue(
            FATUS_QUEUE_NAME,
            function onCreate(err,res){
                if(!err){
                    console.log(MODULE_NAME + ': init-queue created or found with name %s',FATUS_QUEUE_NAME);
                }else{
                    console.error(err);
                }
            });

        console.log(MODULE_NAME + ': init worker pool with max %s ',FATUS_MAX_WORKER);
        this.workerPool = [];

    }


    /**
     * get the instance
     * @returns {*}
     */
    static get instance(){
        if(!this[singleton]) {
            this[singleton] = new Fatusjs(singletonEnforcer);
        }
        return this[singleton];
    }

    /**
     * add a worker to the worker pool
     */
    addWorker(){
        if(this.workerPool.length<FATUS_MAX_WORKER){
            var worker = new FatusWorker(this);
            worker.setRetryTime(FATUS_EQ_RETRY_TIME);
            worker.setMaxAttempts(FATUS_WRK_RETRY_ATTEMP);
            worker.setStackProtection(FATUS_WRK_STACK_TRSHLD);
            worker.setReservationTime(FATUS_JOB_RESERV_TIME);
            this.workerPool.push(worker);
            worker.run();
        }
    }

    /**
     * insert a new msg in the queue
     * @param msg
     * @param onComplete
     */
    insertInQueue(msg,onComplete){
        assert.equal(typeof msg,'object','msg must be an object');
        assert.equal(typeof onComplete,'function','onComplete must be a function');
        let th = this;
        console.log(MODULE_NAME + ': insert new msg');
        this.queueMgr.insertInQueue(
            FATUS_QUEUE_NAME,
            msg,
            function onDone(e,v){
                th.addWorker();
                onComplete(e,v);
            }
        );
    }

    /**
     * get the first element of the queue
     * @param onGet callback function
     */
    getQueueTop(onGet){
        assert.equal(typeof onGet,'function','onGet must be a function');
        this.queueMgr.getMessage(FATUS_QUEUE_NAME,{visibilitytimeout:1},onGet);
    }

    /**
     * get the size of the queue
     * @param onGet
     */
    getQueueSize(onGet){
        assert.equal(typeof onGet,'function','onGet must be a function');
        this.queueMgr.countQueue(FATUS_QUEUE_NAME,onGet);
    }

    /**
     * pop a message from the queue
     * @param msg
     * @param onDelete
     */
    popMsg(msg,onDelete){
        assert.equal(typeof onDelete,'function','onDelete must be a function');
        assert.equal(typeof msg,'object','msg must be an object');
        this.queueMgr.deleteMessage(FATUS_QUEUE_NAME,msg.messageId,msg.popReceipt,{},onDelete);
    }

    /**
     * update a message
     * @param msg
     * @param onUpdate
     */
    updateMsg(msg,onUpdate){
        assert.equal(typeof onUpdate,'function','onUpdate must be a function');
        assert.equal(typeof msg,'object','msg must be an object');
        this.queueMgr.updateMessage(FATUS_QUEUE_NAME,msg.messageId,msg.popReceipt,null,msg,{},onUpdate);
        }

    /**
     * peek at the top of the cue
     * @param onGet function(err,res)
     */
    peekTop(onGet){
        assert.equal(typeof onGet,'function','onGet must be a function');
        this.queueMgr.peekMsg(FATUS_QUEUE_NAME,onGet);
    }


    /**
     * return ALL the cue
     * @param onGet function(err,res)
     */
    getAll(onGet){
        assert.equal(typeof onGet,'function','onGet must be a function');
        this.queueMgr.peekQueue(FATUS_QUEUE_NAME,onGet);
    }

    /**
     * clear all the queue
     * @param onClear function(err,res)
     */
    clear(onClear){
        this.queueMgr.clearQueue(FATUS_QUEUE_NAME,onClear);
    }

    /**
     * get a new messagejob
     * @returns a messageJob object
     */
    createMessageJob(){
        return new MessageJob();
    }

}

/** Exports */
module.exports = Fatusjs;
