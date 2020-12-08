'use strict';

const Fs = require("fs");
const Path = require("path");

const Config = require('./config');
const Logger = require('./logger');
const Study = require('./study');

const Arendel = require('./arendel');
const arendel = new Arendel( Config.getAlfrescoConfig() );

process.title = 'arendel-cli';

var command = process.argv[2];
var studyId = process.argv[3];
var stage = process.argv[4] || Study.STEPS.SIMULATION;

if ( !command || !studyId ) {
    console.log("usage: arendel-cli <command> <studyId> [options]");
    process.exit(1);
}

Logger.info("root directory:", Config.getRootDir() );
Logger.info("studies directory:", Config.getStudiesDir() );

var study = new Study( studyId );

switch ( command ) {
case "compress":
    study.compress( stage ).then(function(filename) {
        console.log("Successfully created file:", filename);
        process.exit(0);
    }, console.log.bind(console) )
    break;

case "uncompress":
    study.uncompress( stage ).then(function() {
        console.log("Extraction successful");
        process.exit(0);
    }, console.log.bind(console) )
    break;

case "upload":
    var filename = Path.resolve( Config.getStudiesDir(), `${study.nodeId}-${stage}.7z` );
    if ( !Fs.existsSync( filename ) ) {
        console.error("File", filename, "not found, aborting.");
        process.exit(1);
    }
    arendel.upload( study.nodeId, "results-000000.7z", filename ).then(function() {
        console.log("Upload successful");
        process.exit(0);

    }, function(err) {
        console.error(err);
        process.exit(2);
    });
    break;

default:
    break;
}
