/**
 * Created by mox on 21/03/16.
 *
 * Fatus worker with priority (on other workers)
 *
 */


'use strict';
const MODULE_NAME = 'FatusPriorityWorker';
const FatusWorker = require('./worker');
const moment = require('moment');

/** the worker for the fatus */
class FatusPriorityWorker extends FatusWorker {

    /**
     * get a new job from the queue NOT FAILED
     * @param th
     * @param wfcallback
     * @override
     */
    fetchNewJob(th, wfcallback) {
        let NOW = moment();
        th.fetchIteration = th.fetchIteration +1;
        if(th.fetchIteration<(th.STACK_PROTECTION_THRSD*2)) {
            th.fatus.getQueueTop(function onGet(err, msg) {
                if (!err && msg && msg[0] && msg[0].messageText) {
                    let reservedCondition = msg[0].messageText.reserved && moment(msg[0].messageText.dtReserve).diff(NOW) < th.MAX_RESERVATION_TIME && msg[0].messageText.reserver != th.name;
                    let notfailedCondition = msg[0].messageText.fail;
                    if (reservedCondition || notfailedCondition){
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
}


/** Exports */
module.exports = FatusPriorityWorker;