"use strict";

var Db = require('../lib/db.js'),
    nconf = require('nconf');

nconf.file(__dirname + '/../config.json');

nconf.defaults({
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': 5432
});

var db = Db.create({
    'user': nconf.get('postgresUser'),
    'password': nconf.get('postgresPassword'),
    'host': nconf.get('postgresHost'),
    'port': nconf.get('postgresPort'),
    'db': nconf.get('postgresDb')
});

db.connect().then(function(){
    db.createTables().then(function(){
        console.log('tables created.');
        db.disconnect();
    }, function(err){
        console.log('error: ' + err);
        db.disconnect();
    });
}, function(err){
    console.log('error: ' + err);
    db.disconnect();
});