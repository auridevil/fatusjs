/**
 * Created by mox on 29/03/16.
 * Fatus worker for collect failed process
 *
 */


'use strict';
const MODULE_NAME = 'FatusCollectorWorker';
const FatusWorker = require('./worker');
const MessageJob = require('./messagejob');
const moment = require('moment');
const async = require('async');
const assert = require('assert');
const util = require('util');
const retry = require('retry');

/** the worker for the fatus */
class FatusCollectorWorker extends FatusWorker {

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
            th.fatus.getQueueTop(function onGet(err, msg) {
                if (!err && msg && msg[0] && msg[0].messageText) {
                    let reservedCondition = msg[0].messageText.reserved && moment(msg[0].messageText.dtReserve).diff(NOW) < th.MAX_RESERVATION_TIME && msg[0].messageText.reserver != th.name;
                    let failCondition = msg[0].messageText.fail < th.MAX_FAIL_ALLOWED;
                    if ( failCondition || reservedCondition) {
                        return th.fetchNewJob(th, wfcallback);
                    }else{
                        wfcallback(err, msg);
                    }
                }else{
                    wfcallback(null,null);
                }
            });
        }else{
            wfcallback(null,null);
        }
    }

    /**
     * execute a single peek-execute-pop cycle
     * @override
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
                        th.fetchIteration = 0;
                        th.fetchNewJob(th,wfcallback);
                    },

                    // reserve the job
                    function reserve(msg, wfcallback){
                        if(msg && msg[0] && msg[0].messageId){
                            msgObj = msg[0];
                            console.log( MODULE_NAME + '%s: msg found, reserving %s',th.name,msgObj.messageId);
                            jobObj = new MessageJob(msgObj);
                            jobObj.reserve(th, wfcallback);
                            th.processing = jobObj;
                            th.processingId = msgObj.messageId;
                        }else {
                            console.log( MODULE_NAME + '%s: all queue elements are not processable -retry later- %s',th.name,util.inspect(msgObj));
                            wfcallback(new Error('queue is empty'),null);
                        }
                    },
                    // pop message to insert into the fail queue
                    function postExecute(res, wfcallback){
                        th.popMessageAndInsert(msgObj,th,wfcallback);
                    }

                ],
                // update if error, else ok
                function _onFinish(err,val){
                    th.processing = null;
                    th.processingId = null;
                    th.emit('runcomplete');
                    // repeat only if stack is not full
                    if(th.iteration<th.STACK_PROTECTION_THRSD && th.fetchIteration<(th.STACK_PROTECTION_THRSD*2)){
                        th.run();
                    }else{
                        console.log(MODULE_NAME + '%s: stack protection threshold, KILLING WORKER',th.name);
                        th.fatus.removeWorker(th.name);
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
     * move message to fail queue
     * @param msg to move
     * @param th ref to this
     * @param callback to call in the end
     */
    popMessageAndInsert(msg,th,callback){
        msg.reserved = false;
        th.fatus.insertInFailQueue(msg,callback);
    }


}


/** Exports */
module.exports = FatusCollectorWorker;