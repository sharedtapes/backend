"use strict";

var express = require('express'),
    Db = require('./db'),
    when = require('when'),
    nconf = require('nconf'),
    util = require('./util'),
    p = require('ua-parser'),
    fs = require('fs'),
    frontend = require('frontend'),
    exfm = require('./exfm');

nconf.file(__dirname + '/../config.json');

nconf.defaults({
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': 5432
});

exfm.injectKeys({
    'exfmAPIKey': nconf.get('exfmAPIKey'),
    'soundcloudAPIKey': nconf.get('soundcloudAPIKey')
});

var app = express(),
    server = require('http').createServer(app),
    io = require('socket.io').listen(server),
    db = Db.create({
        'user': nconf.get('postgresUser'),
        'password': nconf.get('postgresPassword'),
        'host': nconf.get('postgresHost'),
        'port': nconf.get('postgresPort')
    });

var activeSubscriptions = {},
    sockets = {};

// frontend.static is imported from the frontend module
app.use("/static", frontend.static);
app.use(express.bodyParser());

// these should be templated/bootstrapped to prevent excessive AJAXing
app.get('/', function(req, res){
    frontend.template('index.html', {}).then(function(data){
        res.send(data);
    });
});

app.get('/:id', function(req, res){
    frontend.template('index.html', {}).then(function(data){
        res.send(data);
    });
});

// API routes
app.post('/api/v1/tapes', function(req, res){
    db.insertMixtape(res.body).then(function(mixtape){
        res.send(mixtape);
    });
});

app.get('/api/v1/recently-added', function(req, res){
    // db.get('set:recently-added').then(function(tapes){
    //     if (tapes === undefined){
    //         res.send(404);
    //     }
    //     res.send(tapes);
    // });
    res.send([]);
});

app.get('/api/v1/currently-listening', function(req, res){
    // var currentlyListening = [];
    // when.all(Object.keys(activeSubscriptions).map(function(id){
    //     var d = when.defer();
    //     db.get('mixtape:' + id).then(function(tape){
    //         currentlyListening.push({
    //             'id': tape.id,
    //             'title': tape.title
    //         });
    //         d.resolve();
    //     });
    //     return d.promise;
    // })).then(function(){
    //     return res.send(currentlyListening);
    // });
    res.send([]);
});

app.get('/api/v1/tapes/:id', function(req, res){
    db.getMixtape(req.params.id).then(function(mixtape){
        res.send(mixtape);
    }, function(){
        res.send(404);
    });
});

app.put('/api/v1/tapes/:id', function(req, res){
    db.updateMixtape(req.params.id, req.body).then(function(){
        res.send(req.body);
    }, function(){
        res.send(500);
    });
});

app.get('/api/v1/song/:id', function(req, res){
    exfm.getSong(req.params.id).then(function(song){
        res.json({
            'id': song.id,
            'title': song.title,
            'artist': song.artist,
            'url': song.url,
            'tags': song.tags,
            'similar_artists': song.similar_artists,
            'sources': song.sources
        });
    });
});

app.get('/api/v1/search/:query', function(req, res){
    var results = [];
    exfm.search(req.params.query, req.query.start).then(function(songs){
        songs.map(function(song){
            results.push({
                'id': song.id,
                'title': song.title,
                'artist': song.artist,
                'url': song.url,
                'tags': song.tags,
                'similar_artists': song.similar_artists,
                'sources': song.sources
            });
        });
        res.json({
            'results': results
        });
    });
});

// When someone subscribes to a mixtape, add their socket id to that mixtape's
// active subscriptions.
// When someone adds a song to a mixtape, their socket connection will send
// the Mixtape object to the server.  The server will look up that mixtape's
// active subscriptions and loop through all of the socket ids, emitting the new song
// data.

io.configure(function () {
    io.set('flash policy port', -1);
    io.set('transports', ['websocket', 'xhr-polling', 'flashsocket']);
});

io.sockets.on('connection', function(socket){
    sockets[socket.id] = socket;
    socket.on('subscribe', function(sub){
        console.log('subscribing ' + socket.id + ' to ' + sub.id);
        // subscribe socket.id to mixtape sub.id
        if (!activeSubscriptions.hasOwnProperty(sub.id)){
            activeSubscriptions[sub.id] = [];
        }

        if (activeSubscriptions[sub.id].indexOf(socket.id) === -1){
            activeSubscriptions[sub.id].push(socket.id);
        }
        // set the client's current active subscription
        socket.set('activeSubscription', sub.id, function(){
            console.log('saved');
            socket.get('activeSubscription', function(err, mixId){
                console.log('active:' + mixId);
            });
        });

        // update current listeners
        activeSubscriptions[sub.id].map(function(s){
            sockets[s].emit('listeners', activeSubscriptions[sub.id].length);
        });
    });
    socket.on('publish', function(mixtapeString){
        var mixtape = JSON.parse(mixtapeString);
        console.log('new data for ' + mixtape.id + ' from ' + socket.id);
        if (activeSubscriptions.hasOwnProperty(mixtape.id)){
            activeSubscriptions[mixtape.id].map(function(s){
                sockets[s].emit('data', mixtape);
            });
        }
    });
    socket.on('disconnect', function(){
        // remove from global sockets object
        delete sockets[socket.id];
        // unsubscribe socket.id from mixtape sub.id
        socket.get('activeSubscription', function(err, mixId){
            if (mixId !== null){
                // remove from active subscriptions
                console.log('removing ' + socket.id + ' from ' + mixId);
                if (activeSubscriptions[mixId].indexOf(socket.id) !== -1){
                    activeSubscriptions[mixId].splice(activeSubscriptions[mixId].indexOf(socket.id));
                }
                // update current listeners
                activeSubscriptions[mixId].map(function(s){
                    sockets[s].emit('listeners', activeSubscriptions[mixId].length);
                });

                // remove this mixtape from active
                if (!activeSubscriptions[mixId].length){
                    delete activeSubscriptions[mixId];
                }
            }
        });
    });
});

module.exports.start = function(port, cb){
    db.connect().then(function(){
        server.listen(port);
        cb();
    });
};
