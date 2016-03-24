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
        this.iteration = 0;
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
        console.log(MODULE_NAME + '%s: running worker run()',this.name);
        this.fatus.getQueueSize(
            function onSize(err,data){
                if(data && data>0) {
                    console.log(MODULE_NAME + '%s: queue not empty > execute',th.name,th.EQ_RETRY_TIME);
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
        this.iteration = this.iteration+1;
        let msgObj,jobObj;
        try {
            async.waterfall([

                    // get work from queue
                    function top(wfcallback) {
                        console.log(MODULE_NAME + '%s: fetch from queue ', th.name);
                        //th.fetchNewJob(th, wfcallback);
                        th.fetchNewJob(th,wfcallback);
                    },

                    // reserve the job
                    function reserve(msg, wfcallback){
                        assert(typeof msg, 'object', 'msg from queue is null');
                        msgObj = msg[0];
                        if(msgObj && msgObj.messageId) {
                            jobObj = new MessageJob(msgObj);
                            jobObj.reserve(th, wfcallback);
                        }else {
                            wfcallback(new Error('queue is empty'),null);
                        }
                    },

                    // execute instruction in message
                    function msgExecute(res, wfcallback) {
                        console.log(MODULE_NAME + '%s: executing from queue ', th.name);
                        jobObj.execute(th, wfcallback );
                    },

                    // pop message if ok
                    function postExecute(res, wfcallback){
                        th.popMessageFT(msgObj,th,wfcallback);
                    }

                ],
                // update if error, else ok
                function _onFinish(err,val){
                    if(err && typeof err == 'error' && msgObj && jobObj){
                        jobObj.fails();
                        // async error, update message
                        th.updateMsgOnError(jobObj, msgObj, err, th);
                    }
                    th.emit('runcomplete');
                    // repeat only if stack is not full
                    if(th.iteration<th.STACK_PROTECTION_THRSD){
                        th.run();
                    }
                });
        }catch(err){
            // sync error, update message
            if(msgObj && jobObj) {
                jobObj.fails();
                th.updateMsgOnError(jobObj, msgObj, err, th);
            }s
        }
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
                console.log(MODULE_NAME + '%s: update OK');
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
                if(!err && val.popReceipt){
                    msgObj.popReceipt = val.popReceipt;
                }
                onUpdate(err,val);
            }
        );
    }


    /**
     * get a new job from the queue
     * @param th
     * @param wfcallback
     */
    fetchNewJob(th, wfcallback) {
        let NOW = moment();
        th.fatus.getQueueTop(function onGet(err,msg){
            if(!err && msg && msg.messageText) {
                if (msg.messageText.reserved && moment(msg.messageText.dtReserve).diff(NOW) < th.MAX_RESERVATION_TIME && msg.messageText.reserver!=this.name) {
                    return th.fetchNewJob(th, wfcallback);
                }
            }
            wfcallback(err,msg);
        });
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
                        console.log(MODULE_NAME + '%s: err on updateMessage');
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

}

module.exports = FatusWorker;

