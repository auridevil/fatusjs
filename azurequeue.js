/**
 * Created by Aureliano on 21/03/2016.
 */
'use strict';


const DEFUALT_VISIBILITY_TIMEOUT = 2;
const azureQueue = require('azure-queue-node');
const underscore = require('underscore');
const async = require('async');
const assert = require('assert');


/**
 * wrapper for the azure queue for nodejs fatus job processor
 */
class AzureQueue{

    /**
     * default constructor
     */
    constructor (){
        this.queueClient = azureQueue.getDefaultClient();
    }

    /**
     * insert new objects in queue
     * @param queueName
     * @param objects
     * @param onComplete
     */
    insertInQueue(queueName,objects,onComplete){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof onComplete,'function','onComplete must be a function');
        this.queueClient.putMessage(queueName,objects,{},onComplete);

    }

    /**
     * create a new queue by name
     * @param queueName
     * @param onCreate
     */
    createQueue(queueName,onCreate){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof onCreate,'function','onCreate must be a function');
        this.queueClient.createQueue(queueName,onCreate);
    }

    /**
     * get the top message from a queue (reserving it)
     * @param queueName
     * @param option
     * @param onGet
     */
    getMessage(queueName,option,onGet){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof onGet,'function','onGet must be a function');
        var timeOutOption = {visibilityTimeout : DEFUALT_VISIBILITY_TIMEOUT}
        option = underscore.extend(timeOutOption,option);
        this.queueClient.getMessages(
            queueName,
            option,
            onGet
        );
    }

    /**
     * clear all elements in queue
     * @param queueName
     * @param onOk
     */
    clearQueue(queueName,onOk){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof onOk,'function','onOk must be a function');
        this.queueClient.clearMessages(queueName,{},onOk);
    }

    /**
     * counts all elements in queue
     * @param queueName
     * @param onCount
     */
    countQueue(queueName,onCount){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof onCount,'function','onCount must be a function');
        this.queueClient.countMessages(queueName,{},onCount);
    }

    /**
     * delete the queue object
     * @param queueName
     * @param onDone
     */
    deleteQueue(queueName,onDone){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof onDone,'function','onDone must be a function');
        this.queueClient.deleteQueue(queueName,onDone);
    }

    /**
     * delete a single message
     * @param queueName
     * @param messageId
     * @param popReceipt
     * @param options
     * @param onDelete
     */
    deleteMessage(queueName,messageId,popReceipt,options,onDelete){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof messageId,'string','messageId must be a string');
        assert.equal(typeof popReceipt,'string','popReceipt must be a function');
        assert.equal(typeof onDelete,'function','onDelete must be a function');
        this.queueClient.deleteMessage(queueName,messageId,popReceipt,options,onDelete);
    }

    /**
     * update a message in queue
     * @param queueName
     * @param messageId
     * @param popReceipt
     * @param visibilityTimeout
     * @param msg
     * @param options
     * @param onUpdate
     */
    updateMessage(queueName,messageId,popReceipt,visibilityTimeout,msg,options,onUpdate){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof messageId,'string','messageId must be a string');
        assert.equal(typeof popReceipt,'string','popReceipt must be a function');
        assert.equal(typeof msg,'object','msg must be a function');
        assert.equal(typeof onUpdate,'function','onUpdate must be a function');
        this.queueClient.updateMessage(queueName,messageId,popReceipt,visibilityTimeout||DEFUALT_VISIBILITY_TIMEOUT,msg,options,onUpdate);
    }

    /**
     * peek the top of the queue
     * @param queueName
     * @param options
     * @param onGet
     */
    peekMsg(queueName,options,onGet){
        assert.equal(typeof queueName,'string','queueName must be an string');
        this.queueClient.peekMessages(queueName,options,onGet);
    }

    /**
     * peek all the queue
     * @param queueName
     * @param onGet
     */
    peekQueue(queueName,onGet){
        assert.equal(typeof queueName,'string','queueName must be an string');
        assert.equal(typeof onGet,'function','onGet must be a function');
        var thisObj = this;
        async.waterfall([
                function _getCount(wfcallback){
                    thisObj.countQueue(queueName,wfcallback);
                },
                function _putMessage(res,wfcallback){
                    thisObj.peekMsg(queueName,{maxMessages:res},wfcallback);
                },

            ],
            onGet
        )
    }
}

/** Exports */
module.exports = AzureQueue;