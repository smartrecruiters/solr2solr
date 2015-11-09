"use strict";


const _ = require('underscore')

exports._clone = function (doc, toChange, config) {
	let newDoc = _.clone(toChange);
	if (config.clone) {
		for (let property in doc) {
			if (doc.hasOwnProperty(property)) {
				newDoc[property] = doc[property]
			}
		}
	} else {
		for(copyField in config.copy) {
			if(doc[copyField]) newDoc[copyField] = doc[copyField];
		}
	}
	return newDoc;
}

exports._fabricate = function (doc, toChange, config) {
	let newDoc = _.clone(toChange);
	for(let fab of config.fabricate) {
		vals = fab.fabricate(newDoc, start)
		if(vals) newDoc[fab.name] = vals
	}
	return newDoc;
}

exports._transform = function(doc, toChange, config) {
	let newDoc = _.clone(doc);
	for(let transform of config.transform) {
		if (doc[transform.source]) newDoc[transform.destination] = doc[transform.source];
	}
	return newDoc;
}