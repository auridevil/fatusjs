/**
 * Created by mox on 21/03/16.
 */

'use strict';

const EventEmitter = require('events');

class Job{

    constructor (conf){
        this.conf = conf;
    }

    run(){
        this.emit('run');
        var functionPtr = this.conf.execute;
        functionPtr(function onComplete(err,res){
            if(!err){
                this.done();
            }else{
                this.fail();
            }
        });
    }

    done(){
        this.emit('done')
    }

   fail(){
       this.emit('fail');
   }

}

module.exports = Job;