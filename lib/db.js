"use strict";

var pg = require('pg'),
    when = require('when'),
    events = require('events'),
    sequence = require('sequence'),
    util = require('util');

var Db = function(opts){

    // this.recentlyAddedLimit = 100;

    var conString = "tcp://" +
        opts.user + ":" +
        opts.password + "@" +
        opts.host + ":" +
        opts.port + "/" +
        opts.db;

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
    mixtape.similarArtists = [];
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
        this.insertMixtapeRecord(mixtape).then(function(id){
            return next(id);
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next, mixtapeId){
        // *** front end mixtape ID is in base36
        mixtape.id = mixtapeId.toString(36);
        // Create the multivalue string for the songs in the mixtape
        values = this.createSongValues(mixtapeId, mixtape.songs);
        // Run the query to insert all of the songs into songs
        this.insertSongRecord(values).then(function(result){
            return next(result);
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next, result){
        // Create the multivalue string for the similar artists in all the songs
        var songs = [];
        for (var i = 0; i < result.rows.length; i++){
            songs[i] = mixtape.songs[i];
            songs[i].song_id = result.rows[i].song_id;
            mixtape.songs[i].song_id = result.rows[i].song_id;
        }
        values = this.createSimilarArtistsValues(songs);
        // Run the query to insert all of the similar artists into similar_artists
        this.insertSimilarArtistsRecord(values).then(function(result){
            result.rows.map(function(row){
                mixtape.similarArtists.push(row.similar_artist);
            });
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
            return d.resolve(mixtape);
        });
    });
    return d.promise;
};

Db.prototype.updateMixtape = function(mixtape){
    // Need to break the update into different table updates and do a transaction.
    // Mixtapes: update all fields except created and creator
    // Songs: fetch all of the existing songs for this mixtape.
    // Do a comparison: check for new songs and deleted songs.
    // Insert the new songs, insert their similar artists, delete the deleted songs.
    // Update the track order for each existing song.
    var d = when.defer();
    console.log(mixtape);

    sequence(this).then(function(next){
        // Begin the transaction for record update
        this.client.query("BEGIN TRANSACTION", function(err){
            if (err){
                return d.reject(err);
            }
            return next();
        });
    }).then(function(next){
        this.updateMixtapeRecord(parseInt(mixtape.id, 10), mixtape).then(function(){
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        this.selectSongsByMixtapeId(parseInt(mixtape.id, 10)).then(function(result){
            return next(result);
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next, result){

        var songsToUpdate = [],
            songsToDelete = [],
            songsToAdd = [],
            oldSongs = result.rows,
            newSongs = mixtape.songs,
            i;

        for (i = 0; i < oldSongs.length; i++){
            // if this song has been deleted
            if (newSongs.indexOf(oldSongs[i]) === -1){
                // delete it
                songsToDelete.push(oldSongs[i]);
            }
            // otherwise update the old song with the new song's metadata (likely track order will have changed)
            else{
                songsToUpdate.push(newSongs[newSongs.indexOf(oldSongs[i])]);
            }
        }

        for (i = 0; i < newSongs.length; i++){
            // if it doesn't have the song_id prop, it is a new song
            if (!newSongs[i].hasOwnProperty('song_id')){
                songsToAdd.push(newSongs[i]);
            }
        }

        this.updateSongRecord(mixtape).then(function(){
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
            return d.resolve(mixtape);
        });
    });

    d.resolve(mixtape);
    return d.promise;
};

Db.prototype.getMixtape = function(id){
    // Note that this ID will be in base36.  Database PKs are in base 10 (obviously)
    var d = when.defer(),
        mixtape = {
            'id': id,
            'songs': [],
            'similar_artists': []
        },
        songObject = {};
    sequence(this).then(function(next){
        this.selectMixtapeById(parseInt(id, 10)).then(function(result){
            mixtape.title = result.rows[0].title;
            mixtape.author = result.rows[0].creator;
            mixtape.created = result.rows[0].creation_date;
            mixtape.modified = result.rows[0].mod_date;
            mixtape.locked = result.rows[0].locked;
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        this.selectSongsByMixtapeId(parseInt(id, 10)).then(function(result){
            result.rows.map(function(song){
                // In the front end, 'id' refers to the exfm song ID.
                // @todo change this
                songObject = {
                    'id': song.exfm_song_id,
                    'song_id': song.song_id,
                    'title': song.title,
                    'artist': song.artist,
                    'album': song.album,
                    'url': song.url
                };
                if (song.source !== 'null'){
                    songObject.source = song.source;
                }
                mixtape.songs.push(songObject);
            });
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        this.selectSimilarArtistsById(mixtape.songs).then(function(result){
            result.rows.map(function(row){
                if (mixtape.similar_artists.indexOf(row.similar_artist) === -1){
                    mixtape.similar_artists.push(row.similar_artist);
                }
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
    var valuesString = "",
        source = null;
    for (var i = 0; i < songs.length; i++){
        source = null;
        if (songs[i].hasOwnProperty('sources') && songs[i].sources.length){
            source = songs[i].sources[0];
        }
        valuesString +=
            "(" +
                id + ", " + // mixtape id
                "'" + songs[i].id + "', " + // song id
                "'" + songs[i].title + "', " + // song title
                "'" + songs[i].artist + "', " + // song artist
                "'" + songs[i].album + "', " + // song album
                "'" + songs[i].url + "', " + // song url
                "'" + source + "', " + // song source
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
                    "'" + song.song_id + "', " + // song id
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

Db.prototype.insertMixtapeRecord = function(mixtape){
    var d = when.defer();
    this.client.query("INSERT INTO mixtapes (title, creator, creation_date, mod_date, locked) " +
        "VALUES ($1, $2, $3, $4, $5) RETURNING mixtape_id", [
            mixtape.title,
            mixtape.author,
            new Date(mixtape.created),
            new Date(),
            mixtape.locked
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
    this.client.query("INSERT INTO songs (mixtape_id, exfm_song_id, title, artist, album, url, source, track_order) VALUES " +
        values + " RETURNING song_id", function(err, result){
        if (err){
            return d.reject(err);
        }
        return d.resolve(result);
    });
    return d.promise;
};

Db.prototype.insertSimilarArtistsRecord = function(values){
    var d = when.defer();
    this.client.query("INSERT INTO similar_artists (song_id, similar_artist) VALUES " +
        values + " RETURNING similar_artist", function(err, result){
            if (err){
                return d.reject(err);
            }
            return d.resolve(result);
        });
    return d.promise;
};

// Update abstraction methods

Db.prototype.updateMixtapeRecord = function(id, mixtape){
    var d = when.defer();
    this.client.query("UPDATE mixtapes SET title=$1, mod_date=$2, locked=$3 " +
        "WHERE mixtape_id=$3", [mixtape.title, mixtape.modified,
        mixtape.locked, id], function(err){
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
    this.client.query("SELECT title, creator, creation_date, mod_date, locked " +
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
    this.client.query("SELECT song_id, exfm_song_id, title, artist, album, url, source, track_order " +
        "FROM songs WHERE mixtape_id = $1 ORDER BY track_order", [id], function(err, result){
            if (err){
                return d.reject(err);
            }
            return d.resolve(result);
        });
    return d.promise;
};

Db.prototype.selectSimilarArtistsById = function(songs){
    var d = when.defer(),
        whereClause = "";
    for (var i = 0; i < songs.length; i++){
        whereClause += "song_id = '" + songs[i].song_id + "'";
        if (i !== songs.length - 1){
            whereClause += " OR ";
        }
    }
    this.client.query("SELECT similar_artist " +
        "FROM similar_artists WHERE " + whereClause, function(err, result){
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
            "mod_date timestamp, " +
            "locked boolean" +
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
            "song_id serial primary key, " +
            "mixtape_id integer, " +
            "exfm_song_id varchar(40), " +
            "title varchar(40), " +
            "artist varchar(40), " +
            "album varchar(40), " +
            "url varchar(160), " +
            "source varchar(160), " +
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
        "CREATE TABLE similar_artists ( " +
            "song_id integer," +
            "similar_artist varchar(40) " +
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
