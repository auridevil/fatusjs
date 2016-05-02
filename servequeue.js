/**
 * Created by mox on 29/03/16.
 */

'use strict';
var FatusQueue = require('./fatus');
var json2html = require('node-json2html');
var async = require('async');
var util = require('util');


/**
 * work in progress class for presentations purpose
 */
class ServeFatusQueue{

    invoke(req,res){
        let fatusQ = FatusQueue.instance;

        let failQ = null;
        let goodQ = null;
        let workerW = fatusQ.workerPool;
        let priorityW = fatusQ.priorityWorkerPool;
        let failW = fatusQ.failWorkerPool;
        let collW = fatusQ.collectorPool;


        async.parallel([
                function getAllGood(cb){
                    fatusQ.getAll(
                        function onGet(err,val){
                            goodQ = val;
                            cb(err,val);
                        });
                },
                function getAllFail(cb){
                    fatusQ.getAllFail(
                        function onGet(err,val){
                            failQ = val;
                            cb(err,val);
                        }
                    )
                }
            ],
            function onFinish(err,val){
                if(!err){

                    var jtransform = {'tag':'li','html':'${messageId} -  ${insertionTime}: <b> ${messageText.module}/${messageText.function} </b> - ${messageText.fail} '};
                    var wtrasform = {'tag':'li','html':'${name} - ${creation}: IT:[${iteration}] FIT:[${fetchIteration}] PROC:[${processingId}]'};
                    var htmlok = json2html.transform(goodQ,jtransform);
                    var htmlfail = json2html.transform(failQ,jtransform);
                    var htmlw = json2html.transform(workerW,wtrasform);
                    var failw = json2html.transform(failW,wtrasform);
                    var priow = json2html.transform(priorityW,wtrasform);
                    var colw = json2html.transform(collW,wtrasform);

                    var html = '<html><body><div>OK:'  + htmlok + '</div><div>FAIL:'  + htmlfail + '</div><div>WW:' +  htmlw + '</div><div>FW:' + failw + '</div><div>PW:' + priow  + '</div><div>CL:' + colw  + '</div></body></html>';

                    //console.log(html);
                    res.send(html);
                    res.end();
                }else{
                    res.statusCode = 500;
                    res.send(new Error(500));
                    res.end();
                }
            });
    }
}




/** Exports */
module.exports = ServeFatusQueue;