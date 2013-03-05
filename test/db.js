"use strict";

var Db = require('../lib/db.js'),
    sequence = require('sequence'),
    assert = require('assert'),
    nconf = require('nconf');

nconf.file(__dirname + '/../config.json');
var myDatabase = Db.create("tcp://" +
    nconf.get('postgresUser') + ":" +
    nconf.get('postgresPassword') + "@" +
    nconf.get('postgresHost') + ":" +
    nconf.get('postgresPort'));

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
                source: 'http://awesome.com'
            }, {
                id: '456def',
                title: 'Baby',
                artist: 'Justin Bieber',
                album: 'something',
                url: 'http://awesome.com/awesome2.mp3',
                similar_artists: ['Germany Germany', 'Drew Harris'],
                source: 'http://awesome.com'
            }],
            title: 'untitled',
            author: 'anonymous',
            created: '2013-03-04T17:16:37.325Z'
        }).then(function(response){
            done();
        }, function(err){
            console.log(err);
            fail(err, done);
        });
    });

    it("should get a mixtape", function(done){
        myDatabase.getMixtape(1).then(function(response){
            done();
        }, function(err){
            console.log(err);
            fail(err, done);
        });
    });

});
