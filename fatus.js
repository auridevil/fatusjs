
'use strict';

var MODULE_NAME = 'Fatusjs'
var FATUS_QUEUE_NAME = process.env.FATUS_QUEUE_NAME || 'fatusjs-queue';
var FATUS_MAX_WORKER = process.env.FATUS_MAX_WORKER || 5;
var shortid = require('shortid');
var AzureQueue = require('./azurequeue');
var Job = require('./job');
var FatusWorker = require('./worker');

/** singleton */
let singleton = Symbol();
let singletonEnforcer = Symbol();


/**
 * Lightweight azure queue job processor
 */
class Fatusjs{

    /**
     * constructor for the fatus queue
     */
    constructor(enforcer){
        // singleton enforcer pattern
        if(enforcer != singletonEnforcer){
            throw "Cannot construct singleton";
        }
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
        }
    }

    /**
     * insert a new msg in the queue
     * @param msg
     * @param onComplete
     */
    insertInQueue(msg,onComplete){
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
        this.queueMgr.getMessage(FATUS_QUEUE_NAME,{},onGet);
    }

    /**
     * get the size of the queue
     * @param onGet
     */
    getQueueSize(onGet){
        this.queueMgr.countQueue(FATUS_QUEUE_NAME,onGet);
    }

    /**
     * pop a message from the queue
     * @param msg
     * @param onDelete
     */
    popMsg(msg,onDelete){
        this.queueMgr.deleteMessage(FATUS_QUEUE_NAME,msg.messageId,msg.popReceipt,{},onDelete);
    }

    /**
     * peek at the top of the cue
     * @param onGet
     */
    peekTop(onGet){
        this.queueMgr.peekMsg(FATUS_QUEUE_NAME,onGet);
    }


    /**
     * return ALL the cue
     * @param onGet
     */
    getAll(onGet){
        this.queueMgr.peekQueue(FATUS_QUEUE_NAME,onGet);
    }


    //createNewJob(conf){
    //    var j = new Job(conf);
    //    //j.on('done',function(dt,ar){});
    //}


}


module.exports = Fatusjs;