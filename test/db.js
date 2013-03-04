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

	it("should say yo", function(done){
		done();
	});

});
