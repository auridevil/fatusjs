
'use strict';

/** constants */
const MODULE_NAME = 'Fatusjs'
const FATUS_QUEUE_NAME = process.env.FATUS_QUEUE_NAME || 'fatusjs-queue';
const FATUS_QUEUE_FAIL_NAME = process.env.FATUS_QUEUE_NAME || 'fatusjs-queue-fail';
const FATUS_MAX_WORKER = process.env.FATUS_MAX_WORKER || 3;
const FATUS_EQ_RETRY_TIME = process.env.FATUS_EQ_RETRY_TIME || 4000; // millisec
const FATUS_WRK_RETRY_ATTEMP = process.env.FATUS_WRK_RETRY_ATTEMP || 2;
const FATUS_WRK_STACK_TRSHLD = process.env.FATUS_WRK_STACK_TRSHLD || 10;
const FATUS_JOB_RESERV_TIME = process.env.FATUS_JOB_RESERV_TIME || 60; // sec
const FATUS_MAX_FAIL = process.env.FATUS_MAX_FAIL || 10; // max num of fails
const FATUS_MAX_FAIL_TOTAL = process.env.FATUS_MAX_FAIL_TOTAL || 1000; // total number of fails before be suspended

/** inner refs */
const AzureQueue = require('./azurequeue');
const FatusWorker = require('./worker');
const FatusPriorityWorker = require('./priorityworker');
const FatusFailWorker = require('./failworker');
const FatusCollectorWorker = require('./collectorworker');
const MessageJob = require('./messagejob');

/** outher refs */
const assert = require('assert');
const EventEmitter = require('events');
const shortid = require('shortid');
const util = require('util');

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
        this.MessageJob = MessageJob;   // put here to manage outside
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
        this.queueMgr.createQueue(
            FATUS_QUEUE_FAIL_NAME,
            function onCreate(err,res){
                if(!err){
                    console.log(MODULE_NAME + ': init-queue created or found with name %s',FATUS_QUEUE_FAIL_NAME);
                }else{
                    console.error(err);
                }
            });

        console.log(MODULE_NAME + ': init worker pool with max %s ',FATUS_MAX_WORKER);
        this.workerPool = [];
        this.priorityWorkerPool = [];
        this.failWorkerPool = [];
        this.collectorPool = [];
        this.registerMonitor();
        this.registerFailover();
        this.registerAutoWorker();

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
            let worker = new FatusWorker(this);
            this.initWorker(worker);
            this.workerPool.push(worker);
            console.log(MODULE_NAME + ': new worker created %s',worker.name);
            worker.run();
        }else{
            console.log(MODULE_NAME + ': workerpool full, skip adding');
        }
    }

    /**
     * add a worker to the pool
     */
    addPriorityWorker(){
        if(this.priorityWorkerPool.length<10) {
            console.log(MODULE_NAME + ': adding priority worker');
            let worker = new FatusPriorityWorker(this);
            this.initWorker(worker);
            worker.setStackProtection(3);
            this.priorityWorkerPool.push(worker);
            worker.run();
        }
    }

    /**
     * add a worker for the failed jobs pool
     */
    addFailWorker(){
        if(this.failWorkerPool.length<5) {
            let worker = new FatusFailWorker(this);
            this.initWorker(worker);
            worker.setMaxFails(FATUS_MAX_FAIL_TOTAL);
            this.failWorkerPool.push(worker);
            worker.run();
        }
    }

    /**
     * add a collector to the pool
     */
    addCollector(){
        if(this.collectorPool.length<2){
            let worker = new FatusCollectorWorker(this);
            this.initWorker(worker);
            worker.setStackProtection(3);
            this.collectorPool.push(worker);
            worker.run();
        }
    }

    /**
     * init all values for a worker
     * @param worker
     */
    initWorker(worker) {
        worker.setRetryTime(FATUS_EQ_RETRY_TIME);
        worker.setMaxAttempts(FATUS_WRK_RETRY_ATTEMP);
        worker.setStackProtection(FATUS_WRK_STACK_TRSHLD);
        worker.setReservationTime(FATUS_JOB_RESERV_TIME);
        worker.setMaxFails(FATUS_MAX_FAIL);
    }

    /**
     * remove a worker
     * @param name
     */
    removeWorker(name){
        let indx = 0;
        // try remove from worker
        for(let w of this.workerPool){
            if(w.name === name){
                this.workerPool.splice(indx,1);
                return;
            }
            indx ++;
        }
        indx = 0;
        // try remove from priority worker
        for(let w of this.priorityWorkerPool){
            if(w.name === name){
                this.priorityWorkerPool.splice(indx,1);
                return;
            }
            indx ++;
        }
        indx = 0;
        // try remove from fail worker
        for(let w of this.failWorkerPool){
            if(w.name === name){
                this.failWorkerPool.splice(indx,1);
                return;
            }
            indx ++;
        }
        indx = 0;
        for(let w of this.collectorPool){
            if(w.name === name){
                this.collectorPool.splice(indx,1);
                return;
            }
            indx ++;
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
     * insert a new msg in the queue
     * @param msg
     * @param onComplete
     */
    insertInFailQueue(msg,onComplete){
        assert.equal(typeof msg,'object','msg must be an object');
        assert.equal(typeof onComplete,'function','onComplete must be a function');
        let th = this;
        let msgObj = msg;
        console.log(MODULE_NAME + ': moving to fail');
        var insertFunction = function insert(err,res){
            // if(err)//ignore
            th.queueMgr.insertInQueue(
                FATUS_QUEUE_FAIL_NAME,
                msgObj.messageText || msgObj,
                function onDone(e,v){
                    onComplete(e,v);
                }
            );
        };
        try {
            th.popMsg(msgObj, insertFunction);
        }catch(ex){
            insertFunction(null,null);
        }

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
     * get the first element of the fail queue
     * @param onGet callback function
     */
    getFailTop(onGet){
        assert.equal(typeof onGet,'function','onGet must be a function');
        this.queueMgr.getMessage(FATUS_QUEUE_FAIL_NAME,{visibilitytimeout:1},onGet);
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
     * get the size of the fail queue
     * @param onGet
     */
    getFailSize(onGet){
        assert.equal(typeof onGet,'function','onGet must be a function');
        this.queueMgr.countQueue(FATUS_QUEUE_FAIL_NAME,onGet);
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
     * pop a message from the queue
     * @param msg
     * @param onDelete
     */
    popFail(msg,onDelete){
        assert.equal(typeof onDelete,'function','onDelete must be a function');
        assert.equal(typeof msg,'object','msg must be an object');
        this.queueMgr.deleteMessage(FATUS_QUEUE_FAIL_NAME,msg.messageId,msg.popReceipt,{},onDelete);
    }

    /**
     * update a message in the main queue
     * @param msg
     * @param onUpdate
     */
    updateMsg(msg,onUpdate){
        assert.equal(typeof onUpdate,'function','onUpdate must be a function');
        assert.equal(typeof msg,'object','msg must be an object');
        assert.equal(typeof msg.messageText.messageText,'undefined','ECCO LERRORE');
        this.queueMgr.updateMessage(FATUS_QUEUE_NAME,msg.messageId,msg.popReceipt,null,msg.messageText,{},onUpdate);
    }
    /**
     * update a message in the fail queue
     * @param msg
     * @param onUpdate
     */
    updateFail(msg,onUpdate){
        assert.equal(typeof onUpdate,'function','onUpdate must be a function');
        assert.equal(typeof msg,'object','msg must be an object');
        assert.equal(typeof msg.messageText.messageText,'undefined','ECCO LERRORE');
        this.queueMgr.updateMessage(FATUS_QUEUE_FAIL_NAME,msg.messageId,msg.popReceipt,null,msg.messageText,{},onUpdate);
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
     * return ALL the cue
     * @param onGet function(err,res)
     */
    getAllFail(onGet){
        assert.equal(typeof onGet,'function','onGet must be a function');
        this.queueMgr.peekQueue(FATUS_QUEUE_FAIL_NAME,onGet);
    }

    /**
     * clear all the queue
     * @param onClear function(err,res)
     */
    clear(onClear){
        this.queueMgr.clearQueue(FATUS_QUEUE_NAME,onClear);
    }

    /**
     * clear all the queue
     * @param onClear function(err,res)
     */
    clearFail(onClear){
        this.queueMgr.clearQueue(FATUS_QUEUE_FAIL_NAME,onClear);
    }

    /**
     * get a new messagejob
     * @returns a messageJob object
     */
    createMessageJob(){
        return new MessageJob();
    }


    /**
     * staticaly add a new priority worker if the queue is not empty
     */
    static monitor(){
        let fatus = Fatusjs.instance;
        fatus.getQueueSize(function onGet(err,count){
            if(count && count >0){
                fatus.addPriorityWorker();
            }
        });
    }

    /**
     * staticaly add a new fail worker if the fail queue is not empty
     */
    static failRetry(){
        let fatus = Fatusjs.instance;
        fatus.getFailSize(function onGet(err,count){
            if(count && count >0){
                console.log(MODULE_NAME + ': adding fail worker');
                fatus.addFailWorker();
            }
        });
        fatus.getQueueSize(function onGet(err,count){
            if(count && count >0){
                console.log(MODULE_NAME + ': adding collector' )
                fatus.addCollector();
            }
        })
    }

    /**
     * staticaly add a new worker if the queue is not empty
     */
    static autoWorker(){
        let fatus = Fatusjs.instance;
        fatus.getQueueSize(function onGet(err,count){
            if(count && count >0){
                console.log(MODULE_NAME + ': adding fail worker');
                fatus.addWorker();
            }
        });
    }

    /**
     * register a scheduled auto worker
     */
    registerAutoWorker(){
        var functionPtr = Fatusjs.autoWorker;
        if(!this.autoInterval) {
            this.autoInterval = setInterval(
                functionPtr,
                30000
            );
        }
    }

    /**
     * register a scheduled monitoring worker
     */
    registerMonitor(){
        var functionPtr = Fatusjs.monitor;
        if(!this.monitorInterval) {
            this.monitorInterval = setInterval(
                functionPtr,
                120000
            );
        }

    }

    /**
     * register a scheduled fail worker
     */
    registerFailover(){
        var functionPtr = Fatusjs.failRetry;
        if(!this.failInterval) {
            this.failInterval = setInterval(
                functionPtr,
                180000
            );
        }
    }

    /**
     * remove monitoring worker, if any
     */
    unregisterMonitor(){
        if(this.monitorInterval){
            clearInterval(this.monitorInterval);
            this.monitorInterval = false;
        }
    }

    /**
     * remove failover managemnt, if any
     */
    unregisterFailover(){
        if(this.failInterval){
            clearInterval(this.failInterval);
            this.failInterval = false;
        }
    }

    /**
     * remove failover managemnt, if any
     */
    unregisterAutoWorker(){
        if(this.autoInterval){
            clearInterval(this.autoInterval);
            this.autoInterval = false;
        }
    }

}

/** Exports */
module.exports = Fatusjs;
