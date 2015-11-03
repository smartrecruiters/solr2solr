var path    = require('path')
var convert = require('./solr2solrClass')
var yaml 	= require('js-yaml')
var fs 		= require('fs')

// try {
  var uri = path.resolve('config.yml');
  var config = yaml.load(fs.readFileSync(uri), 'utf8');
  // convert.go(config);
  var Solr2Solr = new convert.Solr2Solr(config);
  
  Solr2Solr.processData()
// } catch(err) {
//   console.log("Cannot find config file, do you have one in this directory?");
//   console.log(err);
//   process.exit(1);
// }

