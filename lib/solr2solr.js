var path = require('path')
var solr = require('./solr/solr')
var _ = require('underscore')
var extend = require('util')._extend
var RateLimiter = require('limiter').RateLimiter

var limiter, sourceClient, destClient, config;

var go = function(configuration) {

	config = configuration;

	sourceClient = solr.createClient(config.from)
	destClient   = solr.createClient(config.to)
	
	limiter = new RateLimiter(1, config.throttle)

	config.start = config.start || 0
	config.params = config.params || {}
	config.params = config.params || {}

	nextBatch(configuration.start, configuration.params)
}

var nextBatch = function (start, params) {

	console.log("Querying starting at " + start);

	var newParams = extend(params, {rows: config.rows, start:start});

	limiter.removeTokens(1, function () {

		sourceClient.query (config.query, newParams, function(err, response) {
			

			if (err) console.log("Some kind of solr query error " + err);
			
			var responseObj = JSON.parse(response);
      console.log("Retrieved: " + responseObj.response.docs.length);

			if (config.update) {
				newDocs = prepareDocuments(responseObj.response.docs, start);
        
        console.log("Started Update: ");
				
        updateDocuments(newDocs, function(err, response){
          if(err) console.log("Some kind of solr query error " + err);
          if(response && response.responseHeader) console.log("Query Time: " + response.responseHeader.QTime);
					
          start += config.rows;

					if(responseObj.response.numFound > start){
						nextBatch(start, newParams);
					} else {
						destClient.commit();
					}
				});
			} else {
				newDocs = prepareDocuments(responseObj.response.docs, start)
				addDocuments(newDocs, function() {
					
					start += config.rows

					if (responseObj.response.numFound > start) {
						nextBatch(start, newParams)
					} else {
						destClient.commit()
					}
				});
			}
		});
	});
}



var prepareDocuments = function (docs, start) {

    return docs.map(function(doc){

      var newDoc = {}; 

      if(config.clone){
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
      
      for(fab in config.fabricate){
        vals = fab.fabricate(newDoc, start)
        if(vals) newDoc[fab.name] = vals
      }
      
      for(exclude in config.exclude) {
        delete newDoc[exclude];
      }
      
      start++;
      console.log(newDoc)
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

    destClient.add(_.flatten(docs), function (err) {
      if(err) console.log(err)
      destClient.commit()
      done()
    });
}

var updateDocuments = function (documents, done) {
    var docs = documents.map( function(doc){

        var documentToUpdate = {};
      
        config.update.key.forEach(function(key){
          documentToUpdate[key] = doc[key] || ""
        });

        config.update.copyfield.forEach(function(copyfield) {
          documentToUpdate[copyfield] = {"set": doc[copyfield]}
        });

        return documentToUpdate;
  	})
    // console.log(JSON.stringify(docs));
    destClient.atomicUpdate(docs, done)
}

exports.go = go