"use strict";

var pg = require('pg'),
    when = require('when'),
    events = require('events'),
    sequence = require('sequence'),
    util = require('util');

var Db = function(opts){
    // Pull all of the connection parameters from the options passed to the constructor
    var conString = "tcp://" +
        opts.user + ":" +
        opts.password + "@" +
        opts.host + ":" +
        opts.port + "/" +
        opts.db;
    this.client = new pg.Client(conString);
    this.numberOfRecent = 50;
};
util.inherits(Db, events.EventEmitter);

// "similar" means that having the same song_id
Array.prototype.indexOfSimilar = function(comparison){
    for (var i = 0; i < this.length; i++){
        if (this[i].song_id === comparison.song_id){
            return i;
        }
    }
    return -1;
};

// Callable methods
// Connect to the database
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

// Disconnect from the database
Db.prototype.disconnect = function(){
    this.client.end();
};

// Create the tables for use in production
//   Do each in order, then resolve the promise.
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

// Drop all of the tables
//   Don't do this unless you mean it (really)
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

// Insert a new mixtape into the database.
Db.prototype.insertMixtape = function(mixtape){
    var d = when.defer(),
        values;
    mixtape.similarArtists = [];
    // We need to do a transaction with the following record insertions:
    // table: mixtapes, values: title, creator, creation_date, mod_date
    //   use currval() to get the mixtape_id of the just-added record
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
            this.client.query("ROLLBACK", function(err){
                return d.reject(err);
            });
        });
    }).then(function(next, mixtapeId){
        // *** front end mixtape ID is in base36
        mixtape.id = mixtapeId.toString(36);
        if (!mixtape.songs.length){
            return next();
        }
        // Create the song values string for doing a multi insert into the DB
        values = this.createSongValues(mixtapeId, mixtape.songs);
        // Run the query to insert all of the songs into the songs table
        this.insertSongRecord(values).then(function(result){
            return next(result);
        }, function(err){
            this.client.query("ROLLBACK", function(err){
                return d.reject(err);
            });
        });
    }).then(function(next, result){
        if (result === undefined){
            return next();
        }
        // Create the values string for the similar artists in all the songs
        for (var i = 0; i < result.rows.length; i++){
            mixtape.songs[i].song_id = result.rows[i].song_id;
        }
        values = this.createSimilarArtistsValues(mixtape.songs);
        // Run the query to insert all of the similar artists into the similar_artists table
        this.insertSimilarArtistsRecord(values).then(function(result){
            result.rows.map(function(row){
                // Also add them to the mixtape object for returning to the client
                mixtape.similarArtists.push(row.similar_artist);
            });
            return next();
        }, function(err){
            this.client.query("ROLLBACK", function(err){
                return d.reject(err);
            });
        });
    }).then(function(next){
        // Commit the transaction
        this.client.query("COMMIT TRANSACTION", function(err){
            if (err){
                this.client.query("ROLLBACK", function(err){
                    return d.reject(err);
                });
            }
            return d.resolve(mixtape);
        });
    });
    return d.promise;
};

Db.prototype.updateMixtape = function(id, mixtape){
    var d = when.defer(),
        songsToUpdate = [],
        songsToDelete = [],
        songsToAdd = [],
        oldSongs,
        newSongs,
        values;

    mixtape.id = id;

    sequence(this).then(function(next){
        // Begin the transaction for record update
        this.client.query("BEGIN TRANSACTION", function(err){
            if (err){
                return d.reject(err);
            }
            return next();
        });
    }).then(function(next){
        // Update the mixtape record in the mixtapes table
        // **** remember that frontend mixtape IDs are in base36
        this.updateMixtapeRecord(parseInt(mixtape.id, 36), mixtape).then(function(){
            return next();
        }, function(err){
            this.client.query("ROLLBACK", function(err){
                return d.reject(err);
            });
        });
    }).then(function(next){
        // Get all of the songs for this mixtape from the songs table
        this.selectSongsByMixtapeId(parseInt(mixtape.id, 36)).then(function(result){
            return next(result);
        }, function(err){
            this.client.query("ROLLBACK", function(err){
                return d.reject(err);
            });
        });
    }).then(function(next, result){
        // determine what songs to add, delete, update
        this.updateSongSet(result.rows, mixtape).then(next);
    }).then(function(next){
        // Commit the transaction
        this.client.query("COMMIT TRANSACTION", function(err){
            if (err){
                this.client.query("ROLLBACK", function(err){
                    return d.reject(err);
                });
            }
            return d.resolve(mixtape);
        });
    });
    return d.promise;
};

Db.prototype.getMixtape = function(id){
    // **** frontend mixtape IDs are in base36
    var d = when.defer(),
        mixtape = {
            'id': id,
            'songs': [],
            'similarArtists': []
        },
        songObject = {};
    sequence(this).then(function(next){
        // Get the mixtape from the mixtapes table
        this.selectMixtapeById(parseInt(id, 36)).then(function(result){
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
        // Get the songs associated with this mixtape from the songs table
        this.selectSongsByMixtapeId(parseInt(id, 36)).then(function(result){
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
        // Get the similar artists associated with each of the songs
        // and insert them into the mixtape object
        this.selectSimilarArtistsById(mixtape.songs).then(function(result){
            result.rows.map(function(row){
                if (mixtape.similarArtists.indexOf(row.similar_artist) === -1){
                    mixtape.similarArtists.push(row.similar_artist);
                }
            });
            return d.resolve(mixtape);
        }, function(err){
            return d.reject(err);
        });
    });
    return d.promise;
};

Db.prototype.selectRecentMixtapes = function(){
    // recent means recently modified
    var d = when.defer();
    this.client.query("SELECT mixtape_id, title, creator, creation_date, mod_date, locked " +
        "FROM mixtapes ORDER BY mod_date DESC LIMIT $1", [this.numberOfRecent],
        function(err, result){
            if (err){
                return d.reject();
            }
            return d.resolve(result.rows);
        });
    return d.promise;
};

// INTERNAL HELPER METHODS

Db.prototype.createSongValues = function(id, songs){
    // Create the values string for doing a multi insert into the database
    var valsString = "",
        source,
        trackOrder,
        vals = [];

    for (var i = 0; i < songs.length; i++){
        source = null;
        // if there's no track order explicitly defined in the song object,
        // then assume it is related to its position in the array
        trackOrder = parseInt(i+1, 10);
        // some songs don't have sources (from exfm)
        if (songs[i].hasOwnProperty('sources') && songs[i].sources.length){
            source = songs[i].sources[0];
        }
        if (songs[i].hasOwnProperty('track_order')){
            trackOrder = songs[i].track_order;
        }
        valsString +=
            "(" +
                "$" + ((i*8)+1) + ", " + // mixtape id
                "$" + ((i*8)+2) + ", " + // song id
                "$" + ((i*8)+3) + ", " + // song title
                "$" + ((i*8)+4) + ", " + // song artist
                "$" + ((i*8)+5) + ", " + // song album
                "$" + ((i*8)+6) + ", " + // song url
                "$" + ((i*8)+7) + ", " + // song source
                "$" + ((i*8)+8) + "" + // track order
            ")";
        if (i !== (songs.length - 1)){
            valsString += ", ";
        }
        vals.push(id, songs[i].id, songs[i].title, songs[i].artist,
            songs[i].album, songs[i].url, source, trackOrder);
    }
    return {
        'valsString': valsString,
        'vals': vals
    };
};

// @todo handle string escaping
Db.prototype.createSimilarArtistsValues = function(songs){
    // Create the values string for doing a multi insert into the database
    var valsString = "",
        vals = [],
        song;

    for (var i = 0; i < songs.length; i++){
        song = songs[i];
        if (song.similar_artists.length){
            for (var j = 0; j < song.similar_artists.length; j++){
                song.similar_artists[j] = song.similar_artists[j].replace("'", "''");
                valsString +=
                    "(" +
                        "$" + ((i*2*song.similar_artists.length)+(2*j+1)) + ", " + // song id
                        "$" + ((i*2*song.similar_artists.length)+(2*j+2)) + // artist
                    ")";
                if (i !== (songs.length - 1) ||
                    j !== (song.similar_artists.length - 1)){
                    valsString += ", ";
                }
                vals.push(song.song_id, song.similar_artists[j]);
            }
        }
    }
    return {
        'valsString': valsString,
        'vals': vals
    };
};

// Insertion abstraction methods

Db.prototype.insertMixtapeRecord = function(mixtape){
    // Insert a mixtape record into the table
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
    // Insert multiple song records into the songs table
    var d = when.defer();
    if (!values.vals.length){
        return d.resolve();
    }
    this.client.query("INSERT INTO songs (mixtape_id, exfm_song_id, title, artist, album, url, source, track_order) VALUES " +
        values.valsString + " RETURNING song_id", values.vals, function(err, result){
        if (err){
            return d.reject(err);
        }
        return d.resolve(result);
    });
    return d.promise;
};

Db.prototype.insertSimilarArtistsRecord = function(values){
    // Insert multiple similar artist records into the songs table
    var d = when.defer();
    if (!values.vals.length){
        return d.resolve();
    }
    this.client.query("INSERT INTO similar_artists (song_id, similar_artist) VALUES " +
        values.valsString + " RETURNING similar_artist", values.vals, function(err, result){
            if (err){
                return d.reject(err);
            }
            return d.resolve(result);
        });
    return d.promise;
};

// Update abstraction methods

Db.prototype.updateMixtapeRecord = function(id, mixtape){
    // Update, replacing mod date with now
    var d = when.defer();
    this.client.query("UPDATE mixtapes SET title=$1, mod_date=$2, locked=$3 " +
        "WHERE mixtape_id=$4", [mixtape.title, new Date(),
        mixtape.locked, id], function(err){
            if (err){
                return d.reject(err);
            }
            return d.resolve();
        });
    return d.promise;
};

Db.prototype.updateSongRecord = function(id, songsToUpdate){
    // Do an update for each song added (usually just one)
    var d = when.defer();
    when.all(songsToUpdate.map(function(song){
        var p = when.defer();
        this.client.query("UPDATE songs SET " +
            "track_order = $1 WHERE mixtape_id = $2 AND song_id = $3",
            [song.track_order, id, song.song_id], function(err){
                if (err){
                    return p.reject(err);
                }
                return p.resolve();
            });
        return p.promise;
    }.bind(this))).then(d.resolve, d.reject);
    return d.promise;
};

Db.prototype.updateSongSet = function(oldSongs, mixtape){
    // PROCESS:
    // 1. add the new songs
    // 2. add the similar artists associated with the new songs
    // 3. delete the deleted songs
    // 4. update the old songs that are in the new songset
    var d = when.defer(),
        songsToUpdate = [],
        songsToAdd = [],
        songsToDelete = [],
        i,
        values;
    sequence(this).then(function(next){
        // break the old songset and new songset into to be be added, deleted, updated
        for (i = 0; i < oldSongs.length; i++){
            if (mixtape.songs.indexOfSimilar(oldSongs[i]) > -1){
                songsToUpdate.push(mixtape.songs[mixtape.songs.indexOfSimilar(oldSongs[i])]);
            }
            else {
                songsToDelete.push(oldSongs[i]);
            }
        }
        for (i = 0; i < mixtape.songs.length; i++){
            mixtape.songs[i].track_order = (i+1);
            if (!mixtape.songs[i].hasOwnProperty('song_id')){
                songsToAdd.push(mixtape.songs[i]);
            }
        }
        // 1. add the new songs
        if (!songsToAdd.length){
            return next();
        }
        values = this.createSongValues(parseInt(mixtape.id, 36), songsToAdd);
        this.insertSongRecord(values).then(function(result){
            return next(result);
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next, result){
        // add the song_id for the record just added to the mixtape
        // if there were no songs added, continue
        if (result === undefined){
            return next();
        }
        for (var i = 0; i < result.rows.length; i++){
            mixtape.songs[mixtape.songs.indexOf(songsToAdd[i])].song_id = result.rows[i].song_id;
        }
        // 2. insert similar artists
        values = this.createSimilarArtistsValues(songsToAdd);
        // if there are no similar artists to add
        if (!values.length){
            return next();
        }
        // otherwise add them
        this.insertSimilarArtistsRecord(values).then(function(result){
            result.rows.map(function(row){
                mixtape.similarArtists.push(row.similar_artist);
            });
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        // 3. delete songs
        this.deleteSongRecord(parseInt(mixtape.id, 36), songsToDelete).then(next, d.reject);
    }).then(function(next){
        // 4. update songs
        this.updateSongRecord(parseInt(mixtape.id, 36), songsToUpdate).then(next, d.reject);
    }).then(function(next){
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

// Delete abstraction methods

Db.prototype.deleteSongRecord = function(id, songsToDelete){
    var d = when.defer();
    when.all(songsToDelete.map(function(song){
        var p = when.defer();
        this.client.query("DELETE FROM songs WHERE mixtape_id = $1 AND song_id = $2",
            [id, song.song_id], function(err){
                if (err){
                    return p.reject(err);
                }
                return p.resolve();
            });
        return p.promise;
    }.bind(this))).then(d.resolve, d.reject);
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
            "title text, " +
            "artist text, " +
            "album text, " +
            "url text, " +
            "source text, " +
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
            "similar_artist text " +
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
