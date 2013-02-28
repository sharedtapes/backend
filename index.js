"use strict";

var request = require('superagent'),
    app = require('./lib/app'),
    port = 12370;

app.start(port, function(){
    console.log('listening on ' + port);
});
