"use strict";

var express = require('express'),
    Db = require('./db'),
    when = require('when'),
    nconf = require('nconf'),
    util = require('./util'),
    fs = require('fs'),
    frontend = require('frontend'),
    exfm = require('./exfm');

// Import API keys from local config file and inject into the exfm bridge
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

// Initialize the Express application and the WebSocket server
// Create an instance of the database connection with the credentials
// from the local config
var app = express(),
    server = require('http').createServer(app),
    io = require('socket.io').listen(server),
    db = Db.create({
        'user': nconf.get('postgresUser'),
        'password': nconf.get('postgresPassword'),
        'host': nconf.get('postgresHost'),
        'port': nconf.get('postgresPort'),
        'db': nconf.get('postgresDb')
    });

// In memory stores for the WebSocket connections
var activeSubscriptions = {},
    sockets = {};

// frontend.static is imported from the frontend module,
// which is either pulled from github.com/sharedtapes/frontend on npm install
// or imported locally if frontend has been npm link'd
app.use("/static", frontend.static);
app.use(express.bodyParser());


// 
// API definitions
// 

// Web application routes

// @todo: populate the template with values, rather than having the Backbone
// app handle all of the data loading itself (save an initial XHR)
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

// Insert a mixtape into the database and return
// and updated Mixtape object (will be assigned a unique ID at the stage)
app.post('/api/v1/tapes', function(req, res){
    db.insertMixtape(req.body).then(function(mixtape){
        res.send(JSON.stringify(mixtape));
    }, function(err){
        console.log(err);
        res.send(err);
    });
});
// Grab the most recently added mixtapes for the 'browse' page
app.get('/api/v1/recently-added', function(req, res){
    db.selectRecentMixtapes().then(function(tapes){
        if (tapes === undefined){
            res.send(404);
        }
        res.send(tapes);
    });
});
// Grab all of the active WebSockets and display the mixtapes
// they are viewing
app.get('/api/v1/currently-listening', function(req, res){
    var currentlyListening = [];
    // @todo: bundle up these gets into a single query, as
    // this is kind of stupid
    when.all(Object.keys(activeSubscriptions).map(function(id){
        var d = when.defer();
        db.getMixtape(id).then(function(tape){
            currentlyListening.push({
                'id': tape.getId(),
                'title': tape.getTitle()
            });
            d.resolve();
        });
        return d.promise;
    })).then(function(){
        return res.send(currentlyListening);
    });
});
// Get a mixtape
app.get('/api/v1/tapes/:id', function(req, res){
    db.getMixtape(req.params.id).then(function(mixtape){
        res.send(JSON.stringify(mixtape));
    }, function(err){
        res.send({
            'error': err
        });
    });
});
// Update a mixtape
app.put('/api/v1/tapes/:id', function(req, res){
    // @todo: fix the update, rather than having a 'track_number'
    // column in SQL land, store the tracks as a linked list. would cut
    // down on the excessive update queries just to rearrange the tracks
    // in a mixtape
    db.updateMixtape(req.params.id, req.body).then(function(){
        res.send(req.body);
    }, function(err){
        console.log(err);
        res.send(500);
    });
});
// get a song from exfm's API
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
// run a search on exfm's API
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

// On WebSocket connection, add the connection to the in-memory store
// of subscriptions so that you can view active pages in Browse and
// so that any changes made on any of the active pages will be updated
// for anyone else viewing that page
io.sockets.on('connection', function(socket){
    sockets[socket.id] = socket;

    // subscribe: I am now listening to a mixtape

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

    // publish: I have made a change to a mixtape

    socket.on('publish', function(mixtapeString){
        var mixtape = JSON.parse(mixtapeString);
        console.log('new data for ' + mixtape.id + ' from ' + socket.id);
        if (activeSubscriptions.hasOwnProperty(mixtape.id)){
            activeSubscriptions[mixtape.id].map(function(s){
                // this is to make sure that the publisher doesn't
                // recieve the information again
                if (s.id !== socket.id){
                    sockets[s].emit('data', mixtape);
                }
            });
        }
    });

    // disconnect: I am leaving this mixtape

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

// export a start function to initialize the database connection
// and start the server
module.exports.start = function(port, cb){
    db.connect().then(function(){
        server.listen(port);
        cb();
    });
};
