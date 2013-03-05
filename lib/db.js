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

// CALLABLE METHODS

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
        this.createMixtapesTable().then(function(){
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        this.createSongsTable().then(function(){
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        this.createSimilarArtistsTable().then(function(){
            return d.resolve();
        }, function(err){
            return d.reject(err);
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
    var d = when.defer(),
        values;
    // we need to do a transaction with the following record insertions:
    // table: mixtapes, values: title, creator, creation_date, mod_date
    // use currval() to get the mixtape_id of the just-added record
    // table: mixtape_songs, values: mixtape_id, song_id
    // table: similar_artists, values: song_id, similar_artists
    sequence(this).then(function(next){
        // Begin the transaction for record insertion
        this.client.query("BEGIN TRANSACTION", function(err){
            if (err){
                return d.reject(err);
            }
            return next();
        });
    }).then(function(next){
        // Create the Mixtape record, return the autoincremented id
        this.insertMixtapeRecord(mixtape.title, mixtape.author,
            new Date(mixtape.created), new Date()).then(function(id){
            return next(id);
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next, mixtapeId){
        // Create the multivalue string for the songs in the mixtape
        values = this.createSongValues(mixtapeId, mixtape.songs);
        // Run the query to insert all of the songs into songs
        this.insertSongRecord(values).then(function(){
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        // Create the multivalue string for the similar artists in all the songs
        values = this.createSimilarArtistsValues(mixtape.songs);
        // Run the query to insert all of the similar artists into similar_artists
        this.insertSimilarArtistsRecord(values).then(function(){
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        // Commit the transaction
        this.client.query("COMMIT TRANSACTION", function(err){
            if (err){
                return d.reject(err);
            }
            return d.resolve();
        });
    });
    return d.promise;
};

Db.prototype.updateMixtape = function(){

};

// "mixtape_id integer, " +
// "song_id varchar(40), " +
// "title varchar(40), " +
// "artist varchar(40), " +
// "album varchar(40), " +
// "url varchar(160), " +
// "track_order integer" +

Db.prototype.getMixtape = function(id){
    var d = when.defer(),
        mixtape = {
            'songs': []
        };
    sequence(this).then(function(next){
        this.selectMixtapeById(id).then(function(result){
            mixtape.title = result.rows[0].title;
            mixtape.author = result.rows[0].creator;
            mixtape.created = result.rows[0].creation_date;
            mixtape.modified = result.rows[0].mod_date;
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        this.selectSongsByMixtapeId(id).then(function(result){
            result.rows.map(function(song){
                mixtape.songs.push({
                    'title': song.title,
                    'artist': song.artist,
                    'album': song.album,
                    'url': song.url
                });
            });
            return d.resolve(mixtape);
        }, function(err){
            return d.reject(err);
        });
    });
    return d.promise;
};

// INTERNAL HELPER METHODS

Db.prototype.createSongValues = function(id, songs){
    var valuesString = "";
    for (var i = 0; i < songs.length; i++){
        valuesString +=
            "(" +
                id + ", " + // mixtape id
                "'" + songs[i].id + "', " + // song id
                "'" + songs[i].title + "', " + // song title
                "'" + songs[i].artist + "', " + // song artist
                "'" + songs[i].album + "', " + // song album
                "'" + songs[i].url + "', " + // song url
                parseInt(i+1, 10) + // track order
            ")";
        if (i !== songs.length - 1){
            valuesString += ", ";
        }
    }
    return valuesString;
};

Db.prototype.createSimilarArtistsValues = function(songs){
    var valuesString = "",
        song;
    for (var i = 0; i < songs.length; i++){
        song = songs[i];
        for (var j = 0; j < song.similar_artists.length; j++){
            valuesString +=
                "(" +
                    "'" + song.id + "', " + // song id
                    "'" + song.similar_artists[j] + "'" + // artist
                ")";
            if (i !== (songs.length - 1) ||
                j !== (song.similar_artists.length - 1)){
                valuesString += ", ";
            }
        }
    }
    return valuesString;
};

// Insertion abstraction methods

Db.prototype.insertMixtapeRecord = function(title, artist, created, mod){
    var d = when.defer();
    this.client.query("INSERT INTO mixtapes (title, creator, creation_date, mod_date) " +
        "VALUES ($1, $2, $3, $4) RETURNING mixtape_id", [
            title,
            artist,
            created,
            mod
        ],
        function(err, result){
            if (err){
                return d.reject(err);
            }
            return d.resolve(result.rows[0].mixtape_id);
        });
    return d.promise;
};

Db.prototype.insertSongRecord = function(values){
    var d = when.defer();
    this.client.query("INSERT INTO songs (mixtape_id, song_id, title, artist, album, url, track_order) VALUES " +
        values, function(err){
        if (err){
            return d.reject(err);
        }
        return d.resolve();
    });
    return d.promise;
};

Db.prototype.insertSimilarArtistsRecord = function(values){
    var d = when.defer();
    this.client.query("INSERT INTO similar_artists (song_id, similar_artist) VALUES " +
        values, function(err){
        if (err){
            return d.reject(err);
        }
        return d.resolve();
    });
    return d.promise;
};

// Selection abstraction methods

Db.prototype.selectMixtapeById = function(id){
    var d = when.defer();
    this.client.query("SELECT title, creator, creation_date, mod_date " +
        "FROM mixtapes WHERE mixtape_id = $1", [id], function(err, result){
            if (err){
                return d.reject(err);
            }
            return d.resolve(result);
        });
    return d.promise;
};

Db.prototype.selectSongsByMixtapeId = function(id){
    var d = when.defer();
    this.client.query("SELECT title, artist, album, url, track_order " +
        "FROM songs WHERE mixtape_id = $1 ORDER BY track_order", [id], function(err, result){
            if (err){
                return d.reject(err);
            }
            return d.resolve(result);
        });
    return d.promise;
};

// Table abstraction methods

Db.prototype.createMixtapesTable = function(){
    var d = when.defer();
    this.client.query(
        "CREATE TABLE mixtapes ( " +
            "mixtape_id serial primary key, " +
            "title varchar(40), " +
            "creator varchar(40), " +
            "creation_date timestamp,  " +
            "mod_date timestamp" +
        ")", function(err, result){
        if(err){
            return d.reject(err);
        }
        return d.resolve();
    });
    return d.promise;
};

Db.prototype.createSongsTable = function(){
    var d = when.defer();
    this.client.query(
        "CREATE TABLE songs ( " +
                "mixtape_id integer, " +
                "song_id varchar(40), " +
                "title varchar(40), " +
                "artist varchar(40), " +
                "album varchar(40), " +
                "url varchar(160), " +
                "track_order integer" +
        ")", function(err, result){
        if(err){
            return d.reject(err);
        }
        return d.resolve();
    });
    return d.promise;
};

Db.prototype.createSimilarArtistsTable = function(){
    var d = when.defer();
    this.client.query(
        "CREATE TABLE similar_artists ( "+
            "song_id varchar(40)," +
            "similar_artist varchar(40) "+
        ")",function(err){
        if(err){
            return d.reject(err);
        }
        return d.resolve();
    });
    return d.promise;
};

function create(dbName){
    return new Db(dbName);
}

module.exports = {
    'create': create
};
