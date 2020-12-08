'use strict';

const fs = require('fs');
const path = require('path');

// the root directory of the worker
const rootDir = process.env.WORKER_ROOT_DIR    || path.dirname( __dirname );
const cfgPath = process.env.WORKER_CONFIG_FILE || path.resolve( rootDir, 'conf.json' );

// the config.json file is expected to be located in worker's root directory, ie. in the parent directory
const cfg = JSON.parse( fs.readFileSync( cfgPath ) );
const studiesDir = process.env.WORKER_STUDIES_DIR || path.resolve( ( cfg.baseDir || rootDir ), "studies" );
//console.dir( config );
const binDir = process.env.WORKER_BIN_DIR || path.resolve( rootDir, "bin" );


class Config {

    static getRootDir () {
        return rootDir;
    }

    static getStudiesDir () {
        return studiesDir;
    }

    static getBinDir () {
        return binDir;
    }

    static getAlfrescoConfig () {
        return cfg.alfresco;
    }

    static getActiveMQConfig () {
        return cfg.activemq;
    }

    static getLoggerConfig () {
        return cfg.logger;
    }

    static getAiretdConfig () {
        return cfg.airetd;
    }

}



module.exports = Config;
