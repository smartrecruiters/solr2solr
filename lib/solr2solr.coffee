path = require 'path'
solr = require './solr/solr'
_ = require 'underscore'
extend = require('util')._extend
RateLimiter = require('limiter').RateLimiter

class SolrToSolr

  go: (@config) ->
    @sourceClient = solr.createClient(@config.from)
    @destClient   = solr.createClient(@config.to)
    
    @config.start = @config.start || 0
    @config.params = @config.params || {}
    @config.params = @config.params || {}
    @limiter = new RateLimiter(1, @config.throttle)

    @nextBatch(@config.start, @config.params)

  nextBatch: (start, params) ->
    
    console.log "Querying starting at #{start}"
    
    newParams = extend(params, {rows: @config.rows, start:start});

    @limiter.removeTokens 1, =>
      @sourceClient.query @config.query, newParams, (err, response) =>
        return console.log "Some kind of solr query error #{err}" if err?

        responseObj = JSON.parse response
        if(!@config.update)
          newDocs = @prepareDocuments(responseObj.response.docs, start)
          @addDocuments newDocs, =>
            start += @config.rows
            if responseObj.response.numFound > start
              @nextBatch(start, newParams)
            else
              @destClient.commit()
        else
          newDocs = @prepareDocuments(responseObj.response.docs, start)
          @updateDocuments newDocs, =>
            start += @config.rows
            if responseObj.response.numFound > start
              @nextBatch(start, newParams)
            else
              @destClient.commit()

  prepareDocuments: (docs, start) =>
    for doc in docs
      newDoc = {} 
      if @config.clone
        for cloneField of doc
          newDoc[cloneField] = doc[cloneField]
      else
        for copyField in @config.copy
          newDoc[copyField] = doc[copyField] if doc[copyField]?
      for transform in @config.transform
        newDoc[transform.destination] = doc[transform.source] if doc[transform.source]?
      for fab in @config.fabricate
        vals = fab.fabricate(newDoc, start)
        newDoc[fab.name] = vals if vals?
      for exclude in @config.exclude
        delete newDoc[exclude]

      start++
      newDoc

  addDocuments: (documents, done) ->
    docs = []
    docs.push documents
    if @config.duplicate.enabled
      for doc in documents
        for num in [0..@config.duplicate.numberOfTimes]
          newDoc = _.extend({}, doc)
          newDoc[@config.duplicate.idField] = "#{doc[@config.duplicate.idField]}-#{num}"
          docs.push newDoc

    @destClient.add _.flatten(docs), (err) =>
      console.log err if err
      @destClient.commit()
      done()

  updateDocuments: (documents, done) ->
    docs = []
    for doc in documents
      documentToUpdate = {}
      for key in @config.update.key
        documentToUpdate[key] = doc[key] ? ""
      
      for copyfield in @config.update.copyfield
        documentToUpdate[copyfield] = {"set": doc[copyfield]}
  
      docs.push(documentToUpdate)

    @destClient.atomicUpdate docs, (err) =>
      
      done()

exports.go = (config) ->
  (new SolrToSolr()).go(config)
