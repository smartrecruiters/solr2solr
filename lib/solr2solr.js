var path = require('path')
var solr = require('./solr/solr')
var _ = require('underscore')
var extend = require('util')._extend
var RateLimiter = require('limiter').RateLimiter
var winston = require('winston');

var limiter, config;

//SOLR CLIENTS
var sourceClient, destClient;

var newGeneratedQuery = (config) => return config.query;

var go = function(configuration) {

	config = configuration;

	sourceClient = solr.createClient(config.from)
	destClient   = solr.createClient(config.to)

  	winston.level = 'info';
  	winston.colorize = true;

	limiter = new RateLimiter(1, config.throttle)

	config.start = config.start || 0
	config.params = config.params || {}
	config.params = config.params || {}

	nextBatch(configuration.start, configuration.params)
}



var nextBatch = function (start, params) {

	winston.info(`Querying starting at ${start}`);

	var newParams = extend(params, {rows: config.rows, start:start});
	var generatedQuery = newGeneratedQuery(config);

	limiter.removeTokens(1, function () {
		sourceClient.query(generatedQuery, newParams, function(err, response) {
			
			if (err) winston.error(`Some kind of solr query error ${err}`);
			
			var responseObj = JSON.parse(response);

			winston.info(`Number left to process: ${responseObj.response.numFound}`);

			if (config.update) {
				newDocs = prepareDocuments(responseObj.response.docs, start);
       
			 	winston.info("Started Update");	
        		updateDocuments(newDocs, function(err, response){
          			if(err) winston.error(`Some kind of solr query error ${err}`);
          			if(response && response.responseHeader) winston.info(`Query Time: ${response.responseHeader.QTime}`);

          			start += config.rows;
					if(responseObj.response.numFound > start){
            			destClient.commit(() => nextBatch(start, newParams));
	      			} else {
						if(!responseObj.response.numFound){
							destClient.commit()
						} else {
						 	winston.info("Process Uncommited");	
							start = 0;
							nextBatch(start, newParams);
						}
					}
				});
			} else {
				newDocs = prepareDocuments(responseObj.response.docs, start)
				addDocuments(newDocs, function() {
					
					start += config.rows

					if (responseObj.response.numFound > start) {
						nextBatch(start, newParams)
					} else {
						if(!responseObj.response.numFound){
							destClient.commit()
						} else {
						 	winston.info("Still found not processed. Retry");	
							start = 0;
							nextBatch(start, newParams);
						}
					}
				});
			}
		});
	});
}



var prepareDocuments = function (docs, start) {

    return docs.map((doc) => {
		
		var newDoc = {}; 

		if (config.clone) {
        	for (var property in doc) {
            	if (doc.hasOwnProperty(property)) {
                	newDoc[property] = doc[property]
            	}
        	}
		} else {
        	for(copyField in config.copy) {
				if(doc[copyField]) newDoc[copyField] = doc[copyField];
        	}
		}
      
		for(transform in config.transform) {
			if (doc[transform.source]) newDoc[transform.destination] = doc[transform.source];
      	}
      
		for(fab in config.fabricate) {
        	vals = fab.fabricate(newDoc, start)
			if(vals) newDoc[fab.name] = vals
		}
      
		for(exclude in config.exclude) {
			delete newDoc[exclude];
		}
      
		start++;
		winston.debug(newDoc)
		return newDoc;
    })
   
}

var addDocuments = function (documents, done) {
    var docs = []
    docs.push(documents)

    if(config.duplicate.enabled) {
      for (doc of documents) {
        for (num of [0..config.duplicate.numberOfTimes]){
          newDoc = _.extend({}, doc)
          newDoc[config.duplicate.idField] = doc[config.duplicate.idField] + "-" + num
          docs.push(newDoc)
        }
   	  }
   	}	

    destClient.add(_.flatten(docs), (err) => {
		if(err) winston.error(err)
		destClient.commit()
		done()
    });
}

var updateDocuments = function (documents, done) {
    var docs = documents.map((doc) => {

		var documentToUpdate = {};
      
      	config.update.key.forEach((key) => documentToUpdate[key] = doc[key] || "");
        config.update.copyfield.forEach((copyfield) => documentToUpdate[copyfield] = {"set": doc[copyfield]});

        return documentToUpdate;
  	})

    destClient.atomicUpdate(docs, done)
}

exports.go = go