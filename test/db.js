"use strict";

var Db = require('../lib/db.js'),
    sequence = require('sequence'),
    assert = require('assert'),
    nconf = require('nconf');

nconf.file(__dirname + '/../config.json');

nconf.defaults({
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': 5432,
});

var myDatabase = Db.create({
    'user': nconf.get('postgresUser'),
    'password': nconf.get('postgresPassword'),
    'host': nconf.get('postgresHost'),
    'port': nconf.get('postgresPort'),
    'db': 'sharedtapes_test'
});

// just a quick fail function
var fail = function(err, done){
    assert.equal(null, err);
    done();
};

describe('Database', function(){

    // Do this before running any tests.
    before(function(done){
        myDatabase.connect().then(function(){
            myDatabase.createTables().then(function(){
                return done();
            }, function(err){
                console.log('error: ' + err);
                myDatabase.disconnect();
            });
        }, function(err){
            console.log('error: ' + err);
            myDatabase.disconnect();
        });
    });

    // Do this after running all the tests.
    after(function(done){
        myDatabase.deleteTables().then(function(){
            myDatabase.disconnect();
            return done();
        }, function(err){
            console.log('error: ' + err);
            myDatabase.disconnect();
            return done();
        });
    });

    it("should insert a mixtape", function(done){
        myDatabase.insertMixtape({
            songs: [{
                id: '123abc',
                title: 'Thought Of You',
                artist: 'Justin Bieber',
                album: 'something',
                url: 'http://awesome.com/awesome.mp3',
                similar_artists: ['Germany Germany', 'Brendan Leddy'],
                sources: ['http://awesome2.com']
            }, {
                id: '456def',
                title: 'Baby',
                artist: 'Justin Bieber',
                album: 'something',
                url: 'http://awesome.com/awesome2.mp3',
                similar_artists: ['Germany Germany', "Drew Harris's Band"]
            }],
            title: 'untitled',
            author: 'anonymous',
            created: '2013-03-04T17:16:37.325Z',
            locked: false
        }).then(function(response){
            done();
        }, function(err){
            console.log(err);
            fail(err, done);
        });
    });

    it("should get a mixtape", function(done){
        var id = '1';
        myDatabase.getMixtape(id).then(function(response){
            done();
        }, function(err){
            console.log(err);
            fail(err, done);
        });
    });

    it("should update a mixtape", function(done){
        myDatabase.updateMixtape(1, {
            title: 'my mix',
            author: 'anonymous',
            created: '2013-03-04T17:16:37.325Z',
            modified: '2013-03-07T04:07:10.216Z',
            locked: false,
            similarArtists: [ 'Germany Germany',
                 'Brendan Leddy',
                 'Germany Germany',
                 'Drew Harris'],
            songs: [{
                id: '456def',
                song_id: 2,
                title: 'Baby',
                artist: 'Justin Bieber',
                album: 'something',
                url: 'http://awesome.com/awesome2.mp3',
                similar_artists: ['Germany Germany', 'Drew Harris'],
                source: 'http://awesome.com'
            }, {
                id: '789hij',
                title: 'Another Song',
                artist: 'Justin Bieber',
                album: 'something',
                url: 'http://awesome.com/awesome3.mp3',
                similar_artists: ['Germany Germany', 'Nathan Willson'],
                source: 'http://awesome.com'
            }]
        }).then(function(response){
            done();
        }, function(err){
            console.log(err);
        });
    });

    it("should update the title of a mixtape", function(done){
        myDatabase.updateMixtape(1, {
            title: 'sup',
            author: 'anonymous',
            created: '2013-03-04T17:16:37.325Z',
            modified: '2013-03-07T04:07:10.216Z',
            locked: false,
            similarArtists: [ 'Germany Germany',
                 'Brendan Leddy',
                 'Germany Germany',
                 'Drew Harris'],
            songs: [{
                id: '456def',
                song_id: 2,
                title: 'Baby',
                artist: 'Justin Bieber',
                album: 'something',
                url: 'http://awesome.com/awesome2.mp3',
                similar_artists: ['Germany Germany', 'Drew Harris'],
                source: 'http://awesome.com'
            }, {
                id: '789hij',
                title: 'Another Song',
                artist: 'Justin Bieber',
                album: 'something',
                url: 'http://awesome.com/awesome3.mp3',
                similar_artists: ['Germany Germany', 'Nathan Willson'],
                source: 'http://awesome.com'
            }]
        }).then(function(response){
            done();
        }, function(err){
            console.log(err);
        });
    });

});
