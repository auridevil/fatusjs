fatusjs
=================

Azure queue based job queue for nodejs. Simple and lightweight.

Installation
============

The easiest installation is through [NPM](http://npmjs.org):

    npm install fatusjs
    
Or clone the repo https://github.com/auridevil/fatusjs and include the `./fatus` script.

API
===

Initialize:
    
    var Fatusjs = require('fatusjs');
    var fatusQueue = Fatusjs.instance;
    // fatus is single istance, any call
    // of Fatusjs.instance return the same obj
    fatusQueue.addWorker();

Add a new worker:
    
    fatusQueue.addWorker();
    
Add a new simple job to the queue:

    // create the object
    var msgObjSimple = {
        module   : '/test/simplefunction', // module to require
        function : 'invoke', // function to invoke, in the module
        payload  : {
            // all the data needed by the function
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
    
    // convert to msg
    var jobObj = fatusQueue.createMessageJob();
    jobObj.setSimpleJob(msgObjSimple.module,msgObjSimple.function,msgObjSimple.payload,0,[]);
    var msgJson = jobObj.getMsg();
    
    // add to the queue
    fatusQueue.insertInQueue(msgJson,function onComplete(err,res){
            assert.equal(err,null);
            ...
    })
    
Add a new multistep job to the queue:
    
    // crete the objects
    var step1Obj = { module: '/test/mymodule', function : 'foo' }
    var step2Obj = { module: '/test/mymodule', function : 'bar' }
    var step3Obj = { module: '/test/lib/mymodule', function : 'foo' }
    var payload  = { } // payload can be empty or whatever
    
    // create the job
    var msgJob = fatusQueue.createMessageJob();
    msgJob.setMultiJob();
    
    // add steps
    msgJob.addStep(stepObj1.module,stepObj1.function,payload);
    msgJob.addStep(stepObj2.module,stepObj2.function);
    msgJob.addStep(stepObj3.module,stepObj3.function);
    
    // add to queue
    fatusQueue.insertInQueue(msgJob.getMsg(),function onComplete(err,res){
         assert.equal(err,null);
         ...
    })
    
Configure
=============

In order to work there are some mandatory configurations, all at enviroment level:

    AZURE_STORAGE_ACCOUNT: account name of the azure storage account

    AZURE_STORAGE_ACCESS_KEY: access key of the azure storage account

    CLOUD_STORAGE_ACCOUNT: connection string for the azure storage account

The others configurations is not mandatory:

    FATUS_QUEUE_NAME: name of the queue to be created for the fatus

    FATUS_QUEUE_FAIL_NAME: name of the queue to be crated for the fatus worker failed

    FATUS_MAX_WORKER: max number of worker to be created in the pool (default 3)

    FATUS_EQ_RETRY_TIME: time that a worker waits in order to be killed at empty queue events, in milliseconds (default 4000)

    FATUS_WRK_STACK_TRSHLD: maximum number of iteration of single worker (default 10)
    
    FATUS_MAX_FAIL: maximum number of fail for a job (default 10)

TODO
=============
My ideas for future improvements are:
- create a way to manage "blocked-blocking" jobs
- a visual presentation of the queue(s) state
- extensive testing with multiple instances
- possibility to manage multiple queues


Contributions
=============

If you find bugs or want to change functionality, feel free to fork and pull request.

Notes
=====

The library use the es6 language, please make sure your node version supports it (we currently used 5.7).
The library is still in early stage, please consider some testing.


<i>Cheers from digitalx.</i>
    


    
 