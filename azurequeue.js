/**
 * Created by Aureliano on 21/03/2016.
 */
'use strict';


var DEFUALT_VISIBILITY_TIMEOUT = 480;
var azureQueue = require('azure-queue-node');
var underscore = require('underscore');
var async = require('async');


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
        this.queueClient.putMessage(queueName,objects,{},onComplete);

    }

    /**
     * create a new queue by name
     * @param queueName
     * @param onCreate
     */
    createQueue(queueName,onCreate){
        this.queueClient.createQueue(queueName,onCreate);
    }

    /**
     * get the top message from a queue (reserving it)
     * @param queueName
     * @param option
     * @param onGet
     */
    getMessage(queueName,option,onGet){
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
        this.queueClient.clearMessages(queueName,{},onOk);
    }

    /**
     * counts all elements in queue
     * @param queueName
     * @param onCount
     */
    countQueue(queueName,onCount){
        this.queueClient.countMessages(queueName,{},onCount);
    }

    /**
     * delete the queue object
     * @param queueName
     * @param onDone
     */
    deleteQueue(queueName,onDone){
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
        this.queueClient.deleteMessage(queueName,messageId,popReceipt,options,onDelete);
    }

    /**
     * peek the top of the queue
     * @param queueName
     * @param options
     * @param onGet
     */
    peekMsg(queueName,options,onGet){
        this.queueClient.peekMessages(queueName,options,onGet);
    }

    /**
     * peek all the queue
     * @param queueName
     * @param onGet
     */
    peekQueue(queueName,onGet){
        var thisObj = this;
        async.waterfall([
                function _getCount(wfcallback){
                    thisObj.countQueue(queueName,wfcallback);
                },
                function _putMessage(res,wfcallback){
                    thisObj.peekMsg(queueName,{maxMessages:res},wfcallback);
                }
            ],
            onGet
        )
    }
}

/** Exports */
module.exports = AzureQueue;