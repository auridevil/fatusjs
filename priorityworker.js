/**
 * Created by mox on 21/03/16.
 *
 * Fatus worker with priority (on other workers)
 *
 */


'use strict';
const MODULE_NAME = 'FatusPriorityWorker';
const FatusWorker = require('./worker');

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
        th.fatus.getQueueTop(function onGet(err,msg){
            if(!err && msg && msg.messageText) {
                if (msg.messageText.reserved && moment(msg.messageText.dtReserve).diff(NOW) < th.MAX_RESERVATION_TIME && msg.messageText.reserver!=this.name && (!msg.messageText.fail)) {
                    return th.fetchNewJob(th, wfcallback);
                }
            }
            wfcallback(err,msg);
        });
    }
}


/** Exports */
module.exports = FatusPriorityWorker;