'use strict';

const Config = require('./config');
const logger = require('console-log-level')( Config.getLoggerConfig() || { level: 'info' });
logger.log = logger.info;
module.exports = logger;
