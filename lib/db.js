"use strict";

var pg = require('pg'),
    when = require('when'),
    events = require('events'),
    sequence = require('sequence'),
    util = require('util'),
    Mixtape = require('./models/mixtape.js');

// This is the Database object that will hold the connection to Postgres
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
//   The reason these are all seperated and not
//   just one SQL statement is that I was doing some
//   testing with individual tables.
// @todo: add a createAll method that executes on query
//   to make all the tables
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
    this.client.query("DROP TABLE mixtapes, songs, similar_artists", function(err){
        if(err){
            return d.reject();
        }
        return d.resolve();
    });
    return d.promise;
};

// Insert a new mixtape into the database.
Db.prototype.insertMixtape = function(tape){
    var d = when.defer(),
        values,
        mixtape = Mixtape.create(tape);


    // mixtape is an instance of the Mixtape model that will be holding 
    // all of our data.

    // We need to do a transaction with the following record insertions:
    // table: mixtapes, values: title, creator, creation_date, mod_date
    //   use currval() to get the mixtape_id of the just-added record
    // table: mixtape_songs, values: mixtape_id, song_id
    // table: similar_artists, values: song_id, similar_artists

    // The reason there's so much code here is that the pg driver is async
    // and I'm using callbacks rather than events to keep things tidy.
    // sequence() is similar to asyc.waterfall

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
    }).then(function(next, id){
        // *** front end mixtape ID is in base36 ***
        // @todo: make this more clear. don't even return a base36 ID,
        // let the Backbone app figure that out.
        mixtape.setBase10Id(id);
        mixtape.setId(id.toString(36));
        if (!mixtape.songs.length){
            return next();
        }
        // Create the song values string for doing a multi insert into the DB
        values = mixtape.createSongValues();
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
            mixtape.getSongs()[i].song_id = result.rows[i].song_id;
        }
        values = mixtape.createSimilarArtistsValues();
        if (values.hasOwnProperty('vals') && !values.vals.length){
            return next();
        }
        // Run the query to insert all of the similar artists into the similar_artists table
        this.insertSimilarArtistsRecord(values).then(function(result){
            result.rows.map(function(row){
                // Also add them to the mixtape object for returning to the client
                mixtape.addSimilarArtist(row.similar_artist);
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

// Update a mixtape (runs SQL)
Db.prototype.updateMixtape = function(id, tape){
    var d = when.defer(),
        songsToUpdate = [],
        songsToDelete = [],
        songsToAdd = [],
        oldSongs,
        newSongs,
        values,
        mixtape = Mixtape.create(tape);

    mixtape.setId(id);
    mixtape.setBase10Id(parseInt(mixtape.id, 36));

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
        this.updateMixtapeRecord(mixtape).then(function(){
            return next();
        }, function(err){
            this.client.query("ROLLBACK", function(err){
                return d.reject(err);
            });
        });
    }).then(function(next){
        // Get all of the songs for this mixtape from the songs table
        this.selectSongsByMixtapeId(mixtape.getBase10Id()).then(function(result){
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

// Get a mixtape from the database, organize it into a Mixtape object and return it
Db.prototype.getMixtape = function(id){
    var d = when.defer(),
        mixtape = Mixtape.create({
            'id': id,
            'base10Id': parseInt(id, 36)
        }),
        songObject = {};
    this.selectMultipleJoin(mixtape.getBase10Id()).then(function(result){
        mixtape.setTitle(result.rows[0].mixtape_title);
        mixtape.setAuthor(result.rows[0].creator);
        mixtape.setCreated(result.rows[0].creation_date);
        mixtape.setModified(result.rows[0].mod_date);
        mixtape.setLocked(result.rows[0].locked);
        for (var i = 0; i < result.rows.length; i++){
            songObject = {
                'id': result.rows[i].exfm_song_id,
                'song_id': result.rows[i].song_id,
                'title': result.rows[i].song_title,
                'artist': result.rows[i].artist,
                'album': result.rows[i].album,
                'url': result.rows[i].url
            };
            if (result.rows[i].song_id !== null &&
                mixtape.getSongs().indexOfSimilar(songObject) === -1){
                mixtape.addSong(songObject);
            }
            if (mixtape.getSimilarArtists().indexOf(result.rows[i].similar_artist) === -1){
                mixtape.addSimilarArtist(result.rows[i].similar_artist);
            }
        }
        return d.resolve(mixtape);
    }, d.reject);
    return d.promise;
};

// Get recently added mixtapes
Db.prototype.selectRecentMixtapes = function(){
    // recent means recently modified
    var d = when.defer(),
        recent = [];
    this.client.query("SELECT mixtape_id, title, creator, creation_date, mod_date, locked " +
        "FROM mixtapes ORDER BY mod_date DESC LIMIT $1", [this.numberOfRecent],
        function(err, result){
            if (err){
                return d.reject();
            }
            result.rows.map(function(mixtape){
                recent.push({
                    'mixtape_id': mixtape.mixtape_id.toString(36),
                    'title': mixtape.title,
                    'creator': mixtape.creator,
                    'creation_date': mixtape.creation_date,
                    'mod_date': mixtape.mod_date,
                    'locked': mixtape.locked
                });
            });
            return d.resolve(recent);
        });
    return d.promise;
};

// Insert a mixtape record into the table
Db.prototype.insertMixtapeRecord = function(mixtape){
    var d = when.defer();
    this.client.query("INSERT INTO mixtapes (title, creator, creation_date, mod_date, locked) " +
        "VALUES ($1, $2, $3, $4, $5) RETURNING mixtape_id", [
            mixtape.getTitle(),
            mixtape.getAuthor(),
            new Date(mixtape.created),
            new Date(),
            mixtape.getLocked()
        ],
        function(err, result){
            if (err){
                return d.reject(err);
            }
            return d.resolve(result.rows[0].mixtape_id);
        });
    return d.promise;
};

// Insert multiple song records into the songs table
Db.prototype.insertSongRecord = function(values){
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

// Insert multiple similar artist records into the similar_artists table
Db.prototype.insertSimilarArtistsRecord = function(values){
    var d = when.defer();
    if (values.hasOwnProperty('vals') && !values.vals.length){
        return d.resolve([]);
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

// Update a mixtape record, replacing mod date with now
Db.prototype.updateMixtapeRecord = function(mixtape){
    var d = when.defer();
    this.client.query("UPDATE mixtapes SET title=$1, mod_date=$2, locked=$3 " +
        "WHERE mixtape_id=$4", [mixtape.getTitle(), new Date(),
        mixtape.getLocked(), mixtape.getBase10Id()], function(err){
            if (err){
                return d.reject(err);
            }
            return d.resolve();
        });
    return d.promise;
};

// Song update

// @todo: this is bad. track_order is not a good column to have,
// as re-ordering a mixtape (say of 1000000 songs) would take
// 1000000 queries... the correct answer is to store the tracks as a linked list
// and just update the pointers on track order change.
// i have some code i was working on for this but it's not done.
Db.prototype.updateSongRecord = function(id, songsToUpdate){
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

// Update the song set (no SQL here)
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
            if (mixtape.getSongs().indexOfSimilar(oldSongs[i]) > -1){
                songsToUpdate.push(mixtape.getSongs()[mixtape.getSongs().indexOfSimilar(oldSongs[i])]);
            }
            else {
                songsToDelete.push(oldSongs[i]);
            }
        }
        for (i = 0; i < mixtape.getSongs().length; i++){
            mixtape.getSongs()[i].track_order = (i+1);
            if (!mixtape.getSongs()[i].hasOwnProperty('song_id')){
                songsToAdd.push(mixtape.getSongs()[i]);
            }
        }
        // 1. add the new songs
        if (!songsToAdd.length){
            return next();
        }
        values = mixtape.createSongValues(songsToAdd);
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
            mixtape.getSongs()[
                    mixtape.getSongs().indexOf(songsToAdd[i])
                ].song_id = result.rows[i].song_id;
        }
        // 2. insert similar artists
        values = mixtape.createSimilarArtistsValues(songsToAdd);
        // if there are no similar artists to add
        if (values.hasOwnProperty('vals') && !values.vals.length){
            return next();
        }
        // otherwise add them
        this.insertSimilarArtistsRecord(values).then(function(result){
            result.rows.map(function(row){
                mixtape.addSimilarArtist(row.similar_artist);
            });
            return next();
        }, function(err){
            return d.reject(err);
        });
    }).then(function(next){
        // 3. delete songs
        this.deleteSongRecord(mixtape.getBase10Id(), songsToDelete).then(next, d.reject);
    }).then(function(next){
        // 4. update songs
        this.updateSongRecord(mixtape.getBase10Id(), songsToUpdate).then(next, d.reject);
    }).then(function(next){
        return d.resolve();
    });
    return d.promise;
};

// @todo: rename this
// this does a query on the mixtapes, songs and similar_artists
// tables and returns the data for a full Mixtape object
Db.prototype.selectMultipleJoin = function(id){
    var d = when.defer();
    this.client.query("SELECT " +
        "mixtapes.mixtape_id, " +
        "mixtapes.title AS mixtape_title, " +
        "mixtapes.creator, " +
        "mixtapes.creation_date, " +
        "mixtapes.mod_date, " +
        "mixtapes.locked, " +
        "songs.song_id, " +
        "songs.exfm_song_id, " +
        "songs.title AS song_title, " +
        "songs.artist, " +
        "songs.album, " +
        "songs.url, " +
        "songs.source, " +
        "songs.track_order, " +
        "similar_artists.similar_artist " +
        "FROM mixtapes " +
        "LEFT JOIN songs ON songs.mixtape_id = mixtapes.mixtape_id " +
        "LEFT JOIN similar_artists ON similar_artists.song_id = songs.song_id " +
        "WHERE mixtapes.mixtape_id = $1 " +
        "ORDER BY songs.track_order",
        [id], function(err, result){
            if (err){
                return d.reject(err);
            }
            return d.resolve(result);
        });
    return d.promise;
};

// just get the mixtape info
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

// just get the songs
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

// construct a query statement to select all of the similar artists
// for an array of songs
// @todo clean this up
Db.prototype.selectSimilarArtistsById = function(songs){
    var d = when.defer(),
        whereClause = "";

    if (!songs.length){
        return d.resolve();
    }
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

// delete a song
// @todo: just mark a song as 'deleted' rather than actually delete it...
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

// create the mixtapes table
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

// create the songs table
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

// create the similar artists table
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

// Factory method to create instances of this object
function create(dbName){
    return new Db(dbName);
}
module.exports = {
    'create': create
};
