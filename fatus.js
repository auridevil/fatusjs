
'use strict';

/** constants */
const MODULE_NAME = 'Fatusjs'
const FATUS_QUEUE_NAME = process.env.FATUS_QUEUE_NAME || 'fatusjs-queue';
const FATUS_MAX_WORKER = process.env.FATUS_MAX_WORKER || 5;

/** inner refs */
const AzureQueue = require('./azurequeue');
const FatusWorker = require('./worker');

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
        this.queueMgr.getMessage(FATUS_QUEUE_NAME,{},onGet);
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

}


module.exports = Fatusjs;
//module.exports = FatusWorker;