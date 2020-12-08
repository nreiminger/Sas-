'use strict';

class ConfigurationError extends Error{

    constructor(message) {
      super(message);
      this.name = "ConfigurationError";
    }
}


module.exports = ConfigurationError;
