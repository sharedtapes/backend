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

// Db.prototype.insert = function(key, value){
//     var d = when.defer();
//     this.db.put(key, JSON.stringify(value), function(err){
//         return d.resolve();
//     });
//     return d.promise;
// };

// Db.prototype.insertMixtape = function(id, value){
//     var d = when.defer();
//     this.db.put('mixtape:' + id, JSON.stringify(value), function(err){
//         this.addToRecent({
//             'id': id,
//             'title': value.title,
//             'created': value.created
//         });
//         return d.resolve();
//     }.bind(this));
//     return d.promise;
// };

// Db.prototype.addToRecent = function(value){
//     // keep this.recentlyAddedLimit most recently made
//     // no promise here because the client doesn't care when this finishes executing
//     var data,
//         replace = false;

//     sequence(this).then(function(next){
//         this.db.get('set:recently-added', function(err, value){
//             data = (value === null) ? [] : JSON.parse(value);
//             next(data);
//         });
//     }).then(function(next, recentlyAdded){

//         // first, if this is just a renamed one, update the record in here
//         for (var i = 0; i < recentlyAdded.length; i++){
//             if (recentlyAdded[i].id === value.id){
//                 recentlyAdded.splice(i, 1);
//                 replace = true;
//             }
//         }

//         // if we aren't replacing, we might have to trim
//         if (!replace){
//             // if there are this.recentlyAddedLimit, cut it down to this.recentlyAddedLimit - 1 and
//             // put our new one at the beginning
//             if (recentlyAdded.length > this.recentlyAddedLimit - 1){
//                 recentlyAdded = recentlyAdded.slice(0, this.recentlyAddedLimit - 2);
//             }
//         }

//         recentlyAdded.unshift(value);

//         this.db.put('set:recently-added', JSON.stringify(recentlyAdded), function(err){
//             if (err){
//                 console.log(err);
//             }
//             next();
//         });
//     });
// };

// Db.prototype.get = function(key){
//     var d = when.defer();
//     this.db.get(key, function(err, value){
//         d.resolve(JSON.parse(value));
//     });
//     return d.promise;
// };

// Db.prototype.del = function(key){
//     this.db.del(key);
// };

function create(dbName){
    return new Db(dbName);
}

module.exports = {
    'create': create
};
