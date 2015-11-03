var path = require('path')
var solr = require('./solr/solr')
var _ = require('underscore')
var extend = require('util')._extend
var RateLimiter = require('limiter').RateLimiter
var winston = require('winston');
var Promise = require('bluebird');

var limiter, config;

//SOLR CLIENTS
var sourceClient, destClient;

var newGeneratedQuery = (config) => {return config.query}

var go = function(configuration) {

	config = configuration;

	sourceClient = solr.createClient(config.from)
	destClient   = solr.createClient(config.to)

  winston.level = 'info';
  winston.colorize = true;

	limiter = new RateLimiter(1, config.throttle);

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

		sourceClient.querySolr(generatedQuery, newParams)
			.then((response) => {

				var responseObj = response.body;

				winston.info(`Number left to process: ${responseObj.response.numFound}`);

				var preparedDocuments = preprocessDocuments(responseObj.response.docs, start);

				if (config.update) {
				 	winston.info("Started Update");	

	        		updateDocuments(preparedDocuments)
	    				.then((response) => {
	    					winston.info(`Update successful`);				
		          			winston.info(`Query Time: ${response.body.responseHeader.QTime}`);

		          			start += config.rows;
							
							if(responseObj.response.numFound > start){
		            			destClient.commit(() => {nextBatch(start, newParams);});
			      			} else {
								if(responseObj.response.numFound){
									winston.info("Process Uncommited");	
									nextBatch(0, newParams);
								} else {
									destClient.commit()	
									winston.info("Everything processed");			 	
								}
							}
						})


				} else {
					addDocuments(preparedDocuments)
						.then((response) => {
	    					winston.info(`Addition successful`);				
		          			winston.info(`Query Time: ${response.response.responseHeader.QTime}`);

							start += config.rows;
							
							if (responseObj.response.numFound > start) {
		            			destClient.commit(() => {nextBatch(start, newParams);});
							} else {
								destClient.commit()	
								winston.info("Everything processed");			 	
							}
						}
					);
				}
			});
	});
}


var preprocessDocuments = function (docs, start) {

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
	docs.concat(processDuplication(documents));

    return destClient.addDocument(_.flatten(docs));
}

var processDuplication = function (documents) {
	var docs = [];
    if(config.duplicate.enabled) {
      for (doc of documents) {
        for (num of [0..config.duplicate.numberOfTimes]){
          newDoc = _.extend({}, doc)
          newDoc[config.duplicate.idField] = doc[config.duplicate.idField] + "-" + num
          docs.push(newDoc)
        }
   	  }
   	}	
   	return docs;
}

var updateDocuments = function (documents) {
    var docs = documents.map((doc) => {

		var documentToUpdate = {};
      
      	config.update.key.forEach((key) => documentToUpdate[key] = doc[key] || "");
        config.update.copyfield.forEach((copyfield) => documentToUpdate[copyfield] = {"set": doc[copyfield]});

        return documentToUpdate;
  	})

    return destClient.atomicUpdate(docs)
}

exports.go = go