"use strict";

const path = require('path')
const solr = require('./solr/solr')
const _ = require('underscore')
const extend = require('util')._extend
const RateLimiter = require('limiter').RateLimiter
const winston = require('winston');
const Promise = require('bluebird');


//SOLR CLIENTS
var sourceClient, destClient;


class Solr2Solr {

    constructor(configuration) {
    	this.config = configuration;
		this.sourceClient = solr.createClient(this.config.from)
		this.destClient   = solr.createClient(this.config.to)
		this.limiter = new RateLimiter(1, this.config.throttle);
		this.config.start = this.config.start || 0
		this.config.params = this.config.params || {}
		winston.level = "info"
    }
	
	newGeneratedQuery (config) {return this.config.query}

    processData() {
    	this.nextBatch (this.config.start, this.config.params);
    }

    nextBatch (start, params) {
		winston.info(`Querying starting at ${start}`);

		let newParams = extend(params, {rows: this.config.rows, start:start});
		let generatedQuery = this.newGeneratedQuery(this.config);
		
		var toResolve = Promise.coroutine(function* (val) {
			var queryResponse = yield this.sourceClient.querySolr(generatedQuery, newParams);
			var responseObj = queryResponse.body;

			winston.info(`Number left to process: ${responseObj.response.numFound}`);
			var preparedDocuments = this.preprocessDocuments(responseObj.response.docs, start);

			if (this.config.update) {
			 	winston.info("Started Update");	

    			var updateResponse = yield this.updateDocuments(preparedDocuments);
    			
    			winston.info(`Update successful`);				
  				winston.info(`Query Time: ${updateResponse.body.responseHeader.QTime}`);

      			start += this.config.rows;
				
				if(responseObj.response.numFound >= start){
        			this.destClient.commit(() => {this.nextBatch(start, newParams);});
      			} else {
					if(responseObj.response.numFound){
						winston.info("Process Uncommited");	
						this.nextBatch(0, newParams);
					} else {
						winston.info("Everything processed");			 	
						this.destClient.commit()	
					}
				}
			} else {
				var addDocumentResponse = yield this.addDocuments(preparedDocuments)
		
				winston.info(`Addition successful`);				
      			winston.info(`Query Time: ${response.response.responseHeader.QTime}`);

				start += this.config.rows;
								
				if (responseObj.response.numFound > start) {
        			this.destClient.commit(function() {this.nextBatch(start, newParams);});
				} else {
					this.destClient.commit()	
					winston.info("Everything processed");			 	
				}
			}
		}.bind(this));

		this.limiter.removeTokens(1, toResolve)
	}

	preprocessDocuments (docs, start) {

	    return docs.map((doc) => {
			var newDoc = {}; 

			if (this.config.clone) {
	        	for (var property in doc) {
	            	if (doc.hasOwnProperty(property)) {
	                	newDoc[property] = doc[property]
	            	}
	        	}
			} else {
	        	for(copyField in this.config.copy) {
					if(doc[copyField]) newDoc[copyField] = doc[copyField];
	        	}
			}
	      
			for(transform of this.config.transform) {
				if (doc[transform.source]) newDoc[transform.destination] = doc[transform.source];
	      	}
	      
			for(fab of this.config.fabricate) {
	        	vals = fab.fabricate(newDoc, start)
				if(vals) newDoc[fab.name] = vals
			}
	      
			for(let exclu of this.config.exclude) {
				delete newDoc[exclu];
			}
	      
			start++;
			winston.debug(newDoc)
			return newDoc;
	    })
	   
	}

	addDocuments (documents, done) {
	    var docs = []
	    docs.push(documents)
		docs.concat(processDuplication(documents));

	    return destClient.addDocument(_.flatten(docs));
	}

	processDuplication (documents) {
		var docs = [];
	    if(this.config.duplicate.enabled) {
	      for (doc of documents) {
	        for (num of [0..config.duplicate.numberOfTimes]){
	          newDoc = _.extend({}, doc)
	          newDoc[this.config.duplicate.idField] = doc[this.config.duplicate.idField] + "-" + num
	          docs.push(newDoc)
	        }
	   	  }
	   	}	
	   	return docs;
	}

	updateDocuments (documents) {
	    var docs = documents.map((doc) => {
			var documentToUpdate = {};
	      	this.config.update.key.forEach((key) => documentToUpdate[key] = doc[key] || "");
	        this.config.update.copyfield.forEach((copyfield) => documentToUpdate[copyfield] = {"set": doc[copyfield]});
	        return documentToUpdate;
	  	})

	    return this.destClient.atomicUpdate(docs)
	}

}

exports.Solr2Solr = Solr2Solr