"use strict";

var request = require('superagent'),
    app = require('./lib/app'),
    port = 12370;

// Initialize the application and start the server
app.start(port, function(){
    console.log('listening on ' + port);
});
