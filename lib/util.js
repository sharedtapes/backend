"use strict";

function toBase36(id){
	return id.toString(36);
}

function toBase10(id){
	return parseInt(id, 10);
}

function isSoundcloudUrl(url){
	var domain = url.replace('http://','').replace('https://','').split(/[/?#]/)[0];
	if (domain === 'api.soundcloud.com'){
		return true;
	}
	return false;
}

module.exports = {
	'toBase10': toBase10,
	'toBase36': toBase36,
	'isSoundcloudUrl': isSoundcloudUrl
};