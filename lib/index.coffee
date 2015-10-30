path    = require 'path'
convert = require './solr2solr'
yaml = require('js-yaml')

try
  {config} = yaml.safeLoad(path.resolve 'config.yml', 'utf8'))
catch err
  console.log "Cannot find config file, do you have one in this directory?"
  console.log err
  process.exit(1)

convert.go config