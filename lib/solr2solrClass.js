"use strict";

const path = require('path')
const solr = require('./solr/solr')
const _ = require('underscore')
const extend = require('util')._extend
const RateLimiter = require('limiter').RateLimiter
const winston = require('winston');
const Promise = require('bluebird');

class Solr2Solr {

    constructor (configuration) {
    	this._config = configuration;
		this._sourceClient = solr.createClient(this._config.from)
		this._destClient   = solr.createClient(this._config.to)
		this._limiter = new RateLimiter(1, this._config.throttle);
		this._config.start = this._config.start || 0
		this._config.params = this._config.params || {}
		winston.level = "debug"
    }
	
	_newGeneratedQuery (_config) {return this._config.query}

    processData () {
    	this._nextBatch (this._config.start, this._config.params);
    }

    _nextBatch (start, params) {
		winston.info(`Querying starting at ${start}`);

		let newParams = extend(params, {rows: this._config.rows, start:start});
		let generatedQuery = this._newGeneratedQuery(this._config);
		
		let toResolve = Promise.coroutine(function* (val) {
			let queryResponse = yield this._sourceClient.querySolr(generatedQuery, newParams);
			let responseObj = queryResponse.body;

			winston.info(`Number left to process: ${responseObj.response.numFound}`);
			let preparedDocuments = this._preprocessDocuments(responseObj.response.docs, start);

			if (this._config.update) {
			 	winston.info("Started Update");	

    			let updateResponse = yield this._updateDocuments(preparedDocuments);
    			
    			winston.info(`Update successful`);				
  				winston.info(`Query Time: ${updateResponse.body.responseHeader.QTime}`);

      			start += this._config.rows;
				
				if(responseObj.response.numFound >= start){
        			this._destClient.commit(() => {this._nextBatch(start, newParams);});
      			} else {
					if(responseObj.response.numFound){
						this._destClient.commit(() => {
							winston.info("Process Uncommited");	
							this._nextBatch(0, newParams);
						})	
					} else {
						winston.info("Everything processed");			 	
						this._destClient.commit()	
					}
				}
			} else {
				let addDocumentResponse = yield this._addDocuments(preparedDocuments)
		
				winston.info(`Addition successful`);				
      			winston.info(`Query Time: ${response.response.responseHeader.QTime}`);

				start += this._config.rows;
								
				if (responseObj.response.numFound > start) {
        			this._destClient.commit(function() {this._nextBatch(start, newParams);});
				} else {
					this._destClient.commit()	
					winston.info("Everything processed");			 	
				}
			}
		}.bind(this));

		this._limiter.removeTokens(1, toResolve)
	}

	_preprocessDocuments (docs, start) {
	    return docs.map((doc) => {

		  	let newDoc = this._clone(doc, {});
	      	newDoc = this._transform(doc, newDoc);
	      	newDoc = this._fabricate(doc, newDoc);
			newDoc = _.omit(newDoc, this._config.exclude);

			start++;
			return newDoc;
	    })
	   
	}

	_clone (doc, toChange) {
		let newDoc = _.clone(toChange);
		if (this._config.clone) {
        	for (let property in doc) {
            	if (doc.hasOwnProperty(property)) {
                	newDoc[property] = doc[property]
            	}
        	}
		} else {
        	for(copyField in this._config.copy) {
				if(doc[copyField]) newDoc[copyField] = doc[copyField];
        	}
		}
		return newDoc;	    
	}

	_fabricate (doc, toChange) {
		let newDoc = _.clone(toChange);
		for(let fab of this._config.fabricate) {
    		vals = fab.fabricate(newDoc, start)
			if(vals) newDoc[fab.name] = vals
		}
		return newDoc;
	}

	_transform (doc, toChange) {
		let newDoc = _.clone(doc);
		for(let transform of this._config.transform) {
			if (doc[transform.source]) newDoc[transform.destination] = doc[transform.source];
      	}
      	return newDoc;
	}


	_addDocuments (documents, done) {
	    let docs = []
	    docs.push(documents)
		docs.concat(_processDuplication(documents));
	    return _destClient.addDocument(_.flatten(docs));
	}

	_processDuplication (documents) {
		let docs = [];
	    if(this._config.duplicate.enabled) {
	      for (doc of documents) {
	        for (num of [0.._config.duplicate.numberOfTimes]){
	          newDoc = _.extend({}, doc)
	          newDoc[this._config.duplicate.idField] = doc[this._config.duplicate.idField] + "-" + num
	          docs.push(newDoc)
	        }
	   	  }
	   	}	
	   	return docs;
	}

	_updateDocuments (documents) {
	    let docs = documents.map((doc) => {
			let documentToUpdate = {};
	      	this._config.update.key.forEach((key) => documentToUpdate[key] = doc[key] || "");
	        this._config.update.copyfield.forEach((copyfield) => documentToUpdate[copyfield] = {"set": doc[copyfield]});
	        return documentToUpdate;
	  	})

	    return this._destClient.atomicUpdate(docs)
	}
}

exports.Solr2Solr = Solr2Solr