/**
 * Created by mox on 21/03/16.
 *
 * Fatus worker
 *
 */


'use strict';

const MODULE_NAME = 'FatusWorker';
const MessageJob = require('./messagejob');
const shortid = require('shortid');
const EventEmitter = require('events');
const util = require('util');
const path = require('path');
const assert = require('assert');
const retry = require('retry');
const async = require('async');
const moment = require('moment');

/** the worker for the fatus */
class FatusWorker extends EventEmitter{

    /**
     * constructor
     * @param fatusQueue the reference to the fatus object queue
     */
    constructor (fatusQueue){
        super();
        assert(typeof fatusQueue,'object','Missing fatusQueue in the fatus worker constructor');
        this.name = shortid.generate();
        this.fatus = fatusQueue;
        this.creation = new Date();
        this.iteration = 0;
        this.fetchIteration = 0;
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
        this.failedIteration = 0;
        //console.log(MODULE_NAME + '%s: running worker run()',this.name);
        this.getQueueSize(
            this.fatus,
            function onSize(err,data){
                if(data && data>0) {
                    //console.log(MODULE_NAME + '%s: queue not empty > execute',th.name,th.EQ_RETRY_TIME);
                    console.log(MODULE_NAME + '%s: queue is not empty [%s]',th.name,data);
                    th.single();
                }else if (err){
                    console.error(err);
                }else{
                    if(attempt< th.MAX_WORKER_ATTEMPT) {
                        console.log(MODULE_NAME + '%s: queue is empty, retry in %s',th.name,th.EQ_RETRY_TIME);
                        setTimeout(function(){ th.run(attempt) }, th.EQ_RETRY_TIME)
                    }else{
                        console.log(MODULE_NAME + '%s: queue is empty, KILLING WORKER',th.name);
                        th.fatus.removeWorker(th.name);
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
        // monitor the number of iteration to avoid stack overflows 
        this.iteration = this.iteration+1;
        let msgObj,jobObj;
        try {
            async.waterfall([

                    // get jobs from queue
                    function top(wfcallback) {
                        console.log(MODULE_NAME + '%s: fetch from queue ', th.name);
                        //th.fetchNewJob(th, wfcallback);
                        //monitor the number of fetch iteration to avoid stack overflows
                        th.fetchIteration = 0; 
                        th.fetchNewJob(th,wfcallback);
                    },

                    // reserve the job to execute it
                    function reserve(msg, wfcallback){
                        if(msg && msg[0] && msg[0].messageId){
                            msgObj = msg[0];
                            console.log( MODULE_NAME + '%s: msg found, reserving %s',th.name,msgObj.messageId);
                            
                            // create the messageJob and reserve it
                            jobObj = new MessageJob(msgObj);
                            jobObj.reserve(th, wfcallback);
                            
                            // set the job on the worker
                            th.processing = jobObj;
                            th.processingId = msgObj.messageId;
                        }else {
                            
                            // debugging p
                            th.failedIteration = th.failedIteration+1;
                            if(th.failedIteration%4==0){
                                th.printQueue(th);
                            }else if(th.failedIteration%3==0){
                                console.log( MODULE_NAME + '%s: queue seems empty -retry later- waiting for eventually locked objects',th.name);
                            }
                            wfcallback(new Error('queue is empty'),null);
                        }
                    },

                    // execute instruction in message
                    function msgExecute(res, wfcallback) {
                        th.execute(th, jobObj, wfcallback);
                    },

                    // pop message if ok
                    function postExecute(res, wfcallback){
                        th.popMessageFT(msgObj,th,wfcallback);

                    }

                ],
                // update if error, else ok
                function _onFinish(err,val){
                    // error in execution
                    if(err && typeof err == 'object' && msgObj && jobObj){
                        // async error, update message
                        th.updateMsgOnError(jobObj, msgObj, err, th);
                        console.log(MODULE_NAME + '%s: FAILED JOB  %s', th.name, util.inspect(jobObj.getCompleteMsg()));
                    }else if (err){
                        // inline error from callbacks, to be ignored without msg
                        //console.log('NOT PROPERLY FAILED JOB : %s', util.inspect(msgObj));
                    }else{
                        // nothing to be done
                        //console.log(MODULE_NAME)
                    }
                
                    // deregister job from worker
                    th.processing = null;
                    th.processingId = null;
                    th.emit('runcomplete');
                
                    // repeat only if stack is not full
                    if(th.iteration<th.STACK_PROTECTION_THRSD && th.fetchIteration<(th.STACK_PROTECTION_THRSD*2)){
                        th.run(); // run-> single
                    }else{
                        console.log(MODULE_NAME + '%s: stack protection threshold, KILLING WORKER',th.name);
                        th.fatus.removeWorker(th.name); // killme
                    }
                });
        }catch(err){
            // sync error, update message
            if(msgObj && jobObj) {
                jobObj.fails(err);
                th.updateMsgOnError(jobObj, msgObj, err, th);
            }
        }
    }

    /**
     * print all the queue to console
     * @param th pointer to worker
     */
    printQueue(th) {
        th.fatus.getAll(function onGet(err, res) {
            console.log(MODULE_NAME + '%s: queue is %s', th.name, util.inspect(res, {colors: true}));
        });
    }

    /**
     * overridable execute method
     * @param th pointer to this
     * @param jobObj the jobobj
     * @param wfcallback the callback
     */
    execute(th, jobObj, wfcallback) {
        console.log(MODULE_NAME + '%s: executing from queue ', th.name);
        jobObj.execute(th, wfcallback);
    }

    /**
     * update the message after an error
     * @param msgObj the object
     * @param err the error that caused the code to arrive here
     * @param th the reference to the worker (pointer)
     */
    updateMsgOnError(jobObj, msgObj, err, th) {
        jobObj.fails(err)
        msgObj = jobObj.getCompleteMsg();
        th.updateMessageFT(msgObj, th, function onUpdate(err,res){
            if(err){
                console.log(MODULE_NAME + '%s: FATAL cannot update message');
                console.error(err);
            }else{
                if(res && res.popReceipt){
                    msgObj.popReceipt = res.popReceipt;
                }
                console.log(MODULE_NAME + '%s: update OK',th.name);
            }
        });
    }

    /**
     * update a single message
     * @param msgObj
     * @param onUpdate
     */
    updateMsg(msgObj, onUpdate){
        this.updateMessageFT(
            msgObj,
            this,
            function onDone(err,val){
                if(!err && val && val.popReceipt){
                    msgObj.popReceipt = val.popReceipt;
                }
                onUpdate(err,val);
            }
        );
    }

    /**
     * get the queue size
     * @param fatus
     * @param onGet
     */
    getQueueSize(fatus,onGet){
        fatus.getQueueSize(onGet);
    }


    /**
     * get a new job from the queue
     * @param th
     * @param wfcallback
     */
    fetchNewJob(th, wfcallback) {
        th.fetchIteration = th.fetchIteration +1;
        if(th.fetchIteration<(th.STACK_PROTECTION_THRSD*2)) {
            let NOW = moment();
            th.fatus.getQueueTop(function onGet(err, msg) {
                if (!err && msg && msg[0] && msg[0].messageText) {
                    if (th.isMsgReserved(msg, NOW, th)) {
                        return th.fetchNewJob(th, wfcallback);
                    }else{
                        wfcallback(err, msg);
                    }
                }else {
                    wfcallback(err, null);
                }
            });
        }else{
            wfcallback(null,null);
        }
    }

    /**
     * condition test extracted for override, test if the msg is reserved
     * @param msg
     * @param NOW
     * @param th
     * @returns {URI.characters.reserved|{encode}|*|boolean}
     */
    isMsgReserved(msg, NOW, th) {
        let timeDifference = Math.abs(moment(msg[0].messageText.dtReserve).diff(NOW));
        console.log('TIME DIFFERENCE %s ', timeDifference);
        let reservedCondition = msg[0].messageText.reserved && timeDifference < th.MAX_RESERVATION_TIME && msg[0].messageText.reserver != th.name;
        return reservedCondition;
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
        console.log(MODULE_NAME + th.name + ': POP:[' + util.inspect(msg,{color:true}) + ']' );
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
        var fatus = th.fatus;
        ftOperation.attempt(
            function (currentAttempt){
                fatus.updateMsg(msg,function(err,res){
                    if (ftOperation.retry(err)){
                        //console.log(MODULE_NAME + '%s: err on updateMessage for msg %s ',th.name,msg.messageId);
                        return;
                    }
                    callback(err ? ftOperation.mainError() : null, res);
                })
            });
    }

    /**
     * set the retry time to use in case of an empty queue
     * @param time numeric in millisec
     */
    setRetryTime(time){
        this.EQ_RETRY_TIME = time;
    }

    /**
     * set the max number attempts
     * @param num numeric value
     */
    setMaxAttempts(num){
        this.MAX_WORKER_ATTEMPT = num;
    }

    /**
     * set the stack protection threshold
     * @param maxIteration
     */
    setStackProtection(maxIteration){
        this.STACK_PROTECTION_THRSD = maxIteration;
    }

    /**
     * set the reservation time
     * @param reservationTime num
     */
    setReservationTime(reservationTime){
        this.MAX_RESERVATION_TIME = reservationTime;
    }

    /**
     * set the maximum number of failure that a job allow
     * @param maxFail
     */
    setMaxFails(maxFail){
        this.MAX_FAIL_ALLOWED = maxFail;
    }


}

/** Exports */
module.exports = FatusWorker;

