path    = require 'path'
convert = require './solr2solr'
yaml = require('js-yaml')
fs = require('fs')

try
  console.log path.resolve 'config.yml'
  config = yaml.load(fs.readFileSync(path.resolve 'config.yml'), 'utf8')
catch err
  console.log "Cannot find config file, do you have one in this directory?"
  console.log err
  process.exit(1)

convert.go config