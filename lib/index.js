var path    = require('path')
var convert = require('./solr2solr')
var yaml 	= require('js-yaml')
var fs 		= require('fs')

try {
  var config = yaml.load(fs.readFileSync(path.resolve('config.yml')), 'utf8');
  convert.go(config);
} catch(err) {
  console.log("Cannot find config file, do you have one in this directory?");
  console.log(err);
  process.exit(1);
}

