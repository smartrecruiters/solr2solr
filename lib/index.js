const path      = require('path')
const solr2solr = require('./solr2solrClass')
const yaml 		= require('js-yaml')
const fs 		= require('fs')
const winston 	= require('winston');

try {
  var uri = path.resolve('config.yml');
  var config = yaml.load(fs.readFileSync(uri), 'utf8');
  if(config) {
  	winston.info(`Need to be run with node version >= 4.0. Your version: ${process.version}`)
  	var Solr2Solr = new solr2solr.Solr2Solr(config);
  	Solr2Solr.processData()
  } else {
  	winston.err("Cannot find config file, do you have one in this directory?");
  	process.exit(1);
  }
} catch(err) {
  winston.err(err);
  process.exit(1);
}