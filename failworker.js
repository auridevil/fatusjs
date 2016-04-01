/**
 * Created by mox on 29/03/16.
 * Fatus worker for fail queue
 *
 */

'use strict';
const MODULE_NAME = 'FatusFailWorker';
const FatusWorker = require('./worker');
const MessageJob = require('./messagejob');
const moment = require('moment');
const assert = require('assert');
const util = require('util');
const retry = require('retry');
const async = require('async');


/** the worker for the fatus */
class FatusFailWorker extends FatusWorker {

    /**
     * set the maximum number of failure that a failed job allow
     * @param maxFail
     */
    setMaxFails(maxFail){
        this.MAX_FAIL_ALLOWED = maxFail;
    }

    /**
     * overridable execute method
     * @param th
     * @param jobObj
     * @param wfcallback
     * @override
     */
    execute(th, jobObj, cb) {
        console.log(MODULE_NAME + '%s: executing from fail queue ', th.name);
        if(jobObj.fail>th.MAX_FAIL_ALLOWED){
            console.error(MODULE_NAME + '%s: job %s SUSPENDED for too many fails',th.name,jobObj.getId());
            cb(new Error('job suspended',null));
        }else {
            jobObj.execute(th, cb);
        }
    }

    /**
     * print all the queue to console
     * @param th pointer to worker
     */
    printQueue(th) {
        th.fatus.getAllFail(function onGet(err, res) {
            console.log(MODULE_NAME + '%s: fail queue is %s', th.name, util.inspect(res, {colors: true}));
        });
    }

    /**
     * get the fail queue size
     * @param fatus
     * @param onGet
     * @override
     */
    getQueueSize(fatus,onGet){
        fatus.getFailSize(onGet);
    }

    /**
     * get a new job from the fail queue
     * @param th
     * @param wfcallback
     * @override
     */
    fetchNewJob(th, wfcallback) {
        th.fetchIteration = th.fetchIteration +1;
        if(th.fetchIteration<(th.STACK_PROTECTION_THRSD*2)) {
            let NOW = moment();
            th.fatus.getFailTop(function onGet(err, msg) {
                if (!err && msg && msg[0] && msg[0].messageText) {
                    if(th.isMsgReserved(msg, NOW, th)) {
                        return th.fetchNewJob(th, wfcallback);
                    }else{
                        wfcallback(err, msg);
                    }
                }else{
                    wfcallback(err, null);
                }
            });
        }else{
            wfcallback(null,null);
        }
    }

    /**
     * pop a message from the fail queue, while completed,with fault tollerance
     * @param msg the message to pop
     * @param th the reference to the worker
     * @param callback classic err/res callback
     * @override
     */
    popMessageFT(msg, th, callback){
        th.emit('pop',msg.messageText);
        var ftOperation = retry.operation({
            retries: 10,                    // number of retry times
            factor: 1,                      // moltiplication factor for every rerty
            minTimeout: 1 * 1000,           // minimum timeout allowed
            maxTimeout: 1 * 1000            // maximum timeout allowed
        });
        console.log('======POP: ' + util.inspect(msg));
        ftOperation.attempt(
            function (currentAttempt){
                th.fatus.popFail(msg,function(err,res){
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
     * @override
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
                fatus.updateFail(msg,function(err,res){
                    if (ftOperation.retry(err)){
                        //console.log(MODULE_NAME + '%s: err on updateMessage for msg %s ',th.name,msg.messageId);
                        return;
                    }
                    callback(err ? ftOperation.mainError() : null, res);
                })
            });
    }
}


/** Exports */
module.exports = FatusFailWorker;