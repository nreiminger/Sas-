'use strict';

const Config = require('./config');
const Alfresco = require('./alfresco');
const alfresco = new Alfresco( Config.getAlfrescoConfig() );

const nodeId = "e72baac6-4ea8-4366-bddc-f8841f06a9b0";
const path = "/srv/arendel/worker/studies/e72baac6-4ea8-4366-bddc-f8841f06a9b0-simulation.7z";


alfresco.upload( nodeId, "test-upload.7z", path )
    .then(
        () => console.log("Upload successful"),
        (error) => console.error(error)
    );

