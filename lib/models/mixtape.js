"use strict";

// constructor
var Mixtape = function(mixtape){
	// defaults
	this.songs = mixtape.songs || [];
	this.title = mixtape.title || 'untitled';
	this.author = mixtape.author || 'anonymous';
	this.created = mixtape.created || new Date().toISOString();
	this.similarArtists = mixtape.similarArtists || [];
	this.locked = mixtape.locked || false;
	this.id = mixtape.id || null;
	this.base10Id = mixtape.base10Id || null;
};

// INTERNAL HELPER METHODS

Mixtape.prototype.createSongValues = function(songsOverride){
    // Create the values string for doing a multi insert into the database
    var valsString = "",
        source,
        trackOrder,
        vals = [],
        songs = (songsOverride !== undefined) ? songsOverride : this.songs;

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
        vals.push(this.base10Id, songs[i].id, songs[i].title, songs[i].artist,
            songs[i].album, songs[i].url, source, trackOrder);
    }
    return {
        'valsString': valsString,
        'vals': vals
    };
};

// @todo handle string escaping
Mixtape.prototype.createSimilarArtistsValues = function(songsOverride){
    // Create the values string for doing a multi insert into the database
    var valsString = "",
        vals = [],
        songs = (songsOverride !== undefined) ? songsOverride : this.songs,
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

module.exports.create = function(mixtape){
	return new Mixtape(mixtape);
};