"use strict";

var pg = require('pg').native,
    when = require('when'),
    events = require('events'),
    sequence = require('sequence'),
    util = require('util');

var Db = function(conString){
    // this.recentlyAddedLimit = 100;
    this.client = new pg.Client(conString);
};
util.inherits(Db, events.EventEmitter);

Db.prototype.connect = function(){
    var d = when.defer(); // when is a library that implements promises. Kind of like a callback
    this.client.connect(function(err){
        if (err){
            return d.reject(err); // returns to the rejection case of "then"
        }
        return d.resolve(); // otherwise return to the success case of "then"
    });
    return d.promise;
};

Db.prototype.disconnect = function(){
    this.client.end();
};

Db.prototype.createTables = function(){
    var d = when.defer();
    sequence(this).then(function(next){
        this.client.query(
            "CREATE TABLE mixtapes ( " +
                "pk serial primary key, " +
                "id varchar(40), " +
                "title varchar(40), " +
                "creator varchar(40), " +
                "creation_date timestamp,  " +
                "mod_date timestamp" +
            ")", function(err, result){
            if(err){
                return d.reject(err);
            }
            return next();
        });
    }).then(function(next){
        this.client.query(
            "CREATE TABLE songs ( " +
                "pk serial primary key, " +
                "mixtape_id varchar(40), " +
                "id varchar(40), " +
                "title varchar(40), " +
                "artist varchar(40), " +
                "album varchar(40), " +
                "track_order integer" +
            ")", function(err, result){
            if(err){
                return d.reject(err);
            }
            return next();
        });
    }).then(function(next){
        this.client.query(
            "CREATE TABLE similar_artists ( "+
                "song_id varchar(40)," +
                "artist varchar(40) "+
            ")",function(err){
            if(err){
                return d.reject(err);
            }
            return d.resolve();
        });
    });
    return d.promise;
};

Db.prototype.deleteTables = function(){
    var d = when.defer();
    sequence(this).then(function(next){
        this.client.query("DROP TABLE mixtapes", function(err){
            if(err){
                return d.reject();
            }
            return next();
        });
    }).then(function(next){
        this.client.query("DROP TABLE songs", function(err){
            if(err){
                return d.reject();
            }
            return next();
        });
    }).then(function(next){
        this.client.query("DROP TABLE similar_artists", function(err){
            if(err){
                return d.reject();
            }
            return d.resolve();
        });
    });
    return d.promise;
};

Db.prototype.insertMixtape = function(mixtape){

};

Db.prototype.updateMixtape = function(){

};

Db.prototype.getMixtapeById = function(id){

};

function create(dbName){
    return new Db(dbName);
}

module.exports = {
    'create': create
};
