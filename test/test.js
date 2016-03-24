/**
 * Created by Aureliano on 21/03/2016.
 *
 *
 * THIS TEST NEED HIGH TIMEOUT AND ALL VARS CONFIGURED:
 *
 * --timeout 150000 (mocha env)
 * AZURE_STORAGE_ACCOUNT (env)
 * AZURE_STORAGE_ACCESS_KEY (env)
 * CLOUD_STORAGE_ACCOUNT (env)
 *
 */


var Fatusjs = require('../fatus');
var util = require('util');
var assert = require('assert');
var funct = require('./simplefunction');
var MessageJob = require('./../messagejob');

var msgObjSimple = {
    module   : '/test/simplefunction',
    function : 'invoke',
    payload  : {
        mydata : 'hello',
        who    : 'world',
        arr    : [1,2,3,4,5,6,7,8,9],
        obj    : {
            inner : true,
            desc  : 'inner object'
        }
    },
    isSimple: true
};

var jobObj = new MessageJob();
jobObj.setSimpleJob(msgObjSimple.module,msgObjSimple.function,msgObjSimple.payload,0,[]);

var msgJson = jobObj.getMsg();

var stepObj = {
    module   : '/test/simplefunction',
    function : 'subsInvoke'
}



/*************************** TEST SECTION *************************************/
describe('Init',function(){

    var fatusQueue = Fatusjs.instance;
    var fatus2 = Fatusjs.instance;

    it('should get a fatus instance',function(){
        "use strict";
        assert.equal(typeof fatusQueue,'object');
    })

    it('should be singleton',function(){
        "use strict";
        assert.deepEqual(fatusQueue,fatus2);
    })

    it('should clear the worker and return size 0',function(done){
        "use strict";

        fatusQueue.clear(function onClear(err,res){
            assert.equal(err,null);

            fatusQueue.getQueueSize(function onGet(err,res){
                assert.equal(err,null);
                assert.equal(res,0);
            });
        });

        setTimeout(done,6000);

    })

    it('should add worker',function(done){
        "use strict";
        fatusQueue.addWorker();


        setTimeout(done,10000);
    })

});

describe('Run Processes',function(){

    var fatusQueue = Fatusjs.instance;

    it('should get a fatus instance',function(){
        "use strict";
        assert.equal(typeof fatusQueue,'object');
    })


    it('should insert the job and process it',function(done){
        "use strict";

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);

            setTimeout(function(){
                fatusQueue.getQueueSize(function onGet(err,res){
                    assert.equal(err,null);
                    assert.equal(res,0);
                    console.log('MOCHA: queue is correctly empty')
                });
            },12000);

            setTimeout(done,15000);

        })

    })

    it('should add another worker',function(done){
        "use strict";
        fatusQueue.addWorker();

        setTimeout(done,10000);
    })

});


describe('Load many process',function(){

    var fatusQueue = Fatusjs.instance;

    it('should get a fatus instance',function(){
        "use strict";
        assert.equal(typeof fatusQueue,'object');
    })


    it('should insert many job and not fail',function(done){
        "use strict";

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
        })

        setTimeout(done,15000);


    })

});


describe('use multiStep job operations',function(){
    "use strict";

    var fatusQueue = Fatusjs.instance;

    var payload = {
        dataInfo : 0 ,
        static  : 'fixed value'
    }

    var msgJob = fatusQueue.createMessageJob();
    msgJob.setMultiJob();
    msgJob.addStep(stepObj.module,stepObj.function,payload);
    msgJob.addStep(stepObj.module,stepObj.function);
    msgJob.addStep(stepObj.module,stepObj.function);
    msgJob.addStep(stepObj.module,stepObj.function);
    msgJob.addStep(stepObj.module,stepObj.function);
    msgJob.addStep(stepObj.module,stepObj.function);
    msgJob.addStep(stepObj.module,stepObj.function);
    msgJob.addStep(stepObj.module,stepObj.function);

    it('should get a fatus instance',function(){
        "use strict";
        assert.equal(typeof fatusQueue,'object');
    })

    it('should load a single multi step process and execute',function(done){



        fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
            assert.equal(err,null);
        })

        setTimeout(done,10000);

    })

    it('should load a lot of multi step process and execute',function(done){



        fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
            assert.equal(err,null);
        })
        fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
            assert.equal(err,null);
        })
        fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
            assert.equal(err,null);
        })
        fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
            assert.equal(err,null);
        })
        fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
            assert.equal(err,null);
        })
        fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
            assert.equal(err,null);
        })
        fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
            assert.equal(err,null);
        })
        fatusQueue.addWorker();
        fatusQueue.addWorker();
        fatusQueue.addWorker();


        setTimeout(done,100000);

    });

    it('the queue should be empty',function(done){
        "use strict";

        setTimeout(function(){
            fatusQueue.getQueueSize(function onGet(err,res){
                assert.equal(err,null);
                assert.equal(res,0);
                console.log('queue is correctly empty')
                done();
            });


        })

    })
})
//
//fatusQueue.insertInQueue({msg:'ciaone 1000',obj:{ciao:'ciao'}},function onComplete(err,dat,dat2){
//    "use strict";
//    console.log(util.inspect(dat));
//    console.log(util.inspect(dat2));
//    console.log(util.inspect(err));
//
//})
//
//
//fatusQueue.getQueueTop(function onComplete(err,dat,dat2){
//    "use strict";
//    console.log('GET' + util.inspect(dat));
//    console.log('GET' + util.inspect(dat2));
//    console.log('GET' + util.inspect(err));
//    if(dat && dat[0]){
//        fatusQueue.popMsg(dat[0],function onDelete(err,dat,dat2){
//            console.log('DEL' + util.inspect(dat));
//            console.log('DEL' + util.inspect(dat2));
//            console.log('DEL' + util.inspect(err));
//        })
//    }
//
//})
//
//fatusQueue.getQueueSize(function onComplete(err,dat,dat2){
//    "use strict";
//    console.log('COUNT' + util.inspect(dat));
//    console.log('COUNT' + util.inspect(dat2));
//    console.log('COUNT' + util.inspect(err));
//
//
//})
//
//fatusQueue.peekTop(function onComplete(err,dat,dat2){
//    "use strict";
//    console.log('PEEK' + util.inspect(dat));
//    console.log('PEEK' + util.inspect(dat2));
//    console.log('PEEK' + util.inspect(err));
//});
//
//fatusQueue.getAll(function onComplete(err,dat,dat2){
//    "use strict";
//    console.log('PEEKALL' + util.inspect(dat));
//    console.log('PEEKALL' + util.inspect(dat2));
//    console.log('PEEKALL' + util.inspect(err));
//});
//
