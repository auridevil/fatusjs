/**
 * Created by Aureliano on 21/03/2016.
 */


var Fatusjs = require('../fatus');
var util = require('util');
var fatusQueue = Fatusjs.instance;

//
//fatusQueue.insertInQueue({msg:'ciaone 1'},function onComplete(err,dat,dat2){
//    "use strict";
//    console.log(util.inspect(dat));
//    console.log(util.inspect(dat2));
//    console.log(util.inspect(err));
//
//})
//
//fatusQueue.insertInQueue({msg:'ciaone 2'},function onComplete(err,dat,dat2){
//    "use strict";
//    console.log(util.inspect(dat));
//    console.log(util.inspect(dat2));
//    console.log(util.inspect(err));
//
//})
//
//fatusQueue.insertInQueue({msg:'ciaone 3'},function onComplete(err,dat,dat2){
//    "use strict";
//    console.log(util.inspect(dat));
//    console.log(util.inspect(dat2));
//    console.log(util.inspect(err));
//
//})
//
//fatusQueue.insertInQueue({msg:'ciaone 4'},function onComplete(err,dat,dat2){
//    "use strict";
//    console.log(util.inspect(dat));
//    console.log(util.inspect(dat2));
//    console.log(util.inspect(err));
//
//})
fatusQueue.insertInQueue({msg:'ciaone 1000',obj:{ciao:'ciao'}},function onComplete(err,dat,dat2){
    "use strict";
    console.log(util.inspect(dat));
    console.log(util.inspect(dat2));
    console.log(util.inspect(err));

})

//fatusQueue.insertInQueue(JSON.parse(JSON.stringify(fatusQueue.createNewJob({}))),function onComplete(){console.log('insert job complete')});



fatusQueue.getQueueTop(function onComplete(err,dat,dat2){
    "use strict";
    console.log('GET' + util.inspect(dat));
    console.log('GET' + util.inspect(dat2));
    console.log('GET' + util.inspect(err));
    if(dat && dat[0]){
        fatusQueue.popMsg(dat[0],function onDelete(err,dat,dat2){
            console.log('DEL' + util.inspect(dat));
            console.log('DEL' + util.inspect(dat2));
            console.log('DEL' + util.inspect(err));
        })
    }

})

fatusQueue.getQueueSize(function onComplete(err,dat,dat2){
    "use strict";
    console.log('COUNT' + util.inspect(dat));
    console.log('COUNT' + util.inspect(dat2));
    console.log('COUNT' + util.inspect(err));


})

fatusQueue.peekTop(function onComplete(err,dat,dat2){
    "use strict";
    console.log('PEEK' + util.inspect(dat));
    console.log('PEEK' + util.inspect(dat2));
    console.log('PEEK' + util.inspect(err));
});

fatusQueue.getAll(function onComplete(err,dat,dat2){
    "use strict";
    console.log('PEEKALL' + util.inspect(dat));
    console.log('PEEKALL' + util.inspect(dat2));
    console.log('PEEKALL' + util.inspect(err));
});


//
//
//DefaultEndpointsProtocol=https;AccountName=dgxsa001;AccountKey=SGbNPjwE83Amf60nF3LihUdKCQTmBIjvGVjNDJkRCWtjbD2u2O/BaHHCQhF+1nBs+m4eKca893Hl+oxeADXgDQ==;BlobEndpoint=https://dgxsa001.blob.core.windows.net/;TableEndpoint=https://dgxsa001.table.core.windows.net/;;FileEndpoint=https://dgxsa001.file.core.windows.net/
//
//QueueEndpoint=https://pizzayoudata.queue.core.windows.net/;AccountName=pizzayoudata;AccountKey=SGbNPjwE83Amf60nF3LihUdKCQTmBIjvGVjNDJkRCWtjbD2u2O/BaHHCQhF+1nBs+m4eKca893Hl+oxeADXgDQ==