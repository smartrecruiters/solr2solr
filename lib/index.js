var path    = require('path')
var convert = require('./solr2solr')
var yaml 	= require('js-yaml')
var fs 		= require('fs')

try {
  var uri = path.resolve('config.yml');
  console.log(uri)
  var config = yaml.load(fs.readFileSync(uri), 'utf8');
  convert.go(config);
} catch(err) {
  console.log("Cannot find config file, do you have one in this directory?");
  console.log(err);
  process.exit(1);
}

