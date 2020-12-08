'use strict';

const Stomp = require('stomp-client');
const util = require('util');
const setTimeoutPromise = util.promisify(setTimeout);

const Config = require('./config');
const Logger = require('./logger');
const Study = require('./study');

const queueSimulation = '/queue/simulation';

process.title = 'arendel-worker';

Logger.info("root directory:", Config.getRootDir() );
Logger.info("studies directory:", Config.getStudiesDir() );

const CMD_START_MESHING = 'start-meshing';
const CMD_ABORT_MESHING = 'abort-meshing';
const CMD_START_SIMULATION = 'start-simulation';
const CMD_ABORT_SIMULATION = 'abort-simulation';
const CMD_START_POSTPROC= 'start-postproc';
const CMD_ABORT_POSTPROC= 'abort-postproc';


function parseMessage ( rawMsg ) {
    try {
        var msg = JSON.parse( rawMsg );

        var study = new Study( msg.nodeRef );

        switch ( msg.cmd ) {
        case CMD_START_MESHING:
            // we need to wait a couple of seconds because alfresco may be slow to update the meshing status
            setTimeoutPromise( 2000 )
                .then( () => study.startMeshing() )
                .then(
                    ()    => Logger.info("Meshing done"),
                    (err) => Logger.error( err )
                );
            break;

        case CMD_ABORT_MESHING:
            study.abortMeshing().then( () => Logger.info("Meshing aborted"), (err) => Logger.error( err ) );
            break;

        case CMD_START_SIMULATION:
            // we need to wait a couple of seconds because alfresco may be slow to create the simulation node
            setTimeoutPromise( 2000 )
                .then( () => study.startSimulation( msg.simNodeRef ) )
                .then( () => Logger.info("Simulation done"), (err) => Logger.error( err ) );
            break;

        case CMD_ABORT_SIMULATION:
            study.abortSimulation( msg.simNodeRef ).then( () => Logger.info("Simulation aborted"), (err) => Logger.error( err ) );
            break;

        case CMD_START_POSTPROC:
            // we need to wait a couple of seconds because alfresco may be slow to create the simulation node
            setTimeoutPromise( 2000 )
                .then( () => study.postproc( msg.simNodeRef ) )
                .then( () => Logger.info("Post-processing done"), (err) => Logger.error( err ) );
            break;

        case CMD_ABORT_POSTPROC:
            study.abortPostproc().then( () => Logger.info("Postprocessing aborted"), (err) => Logger.error( err ) );
            break;
        }
    } catch (e) {
        Logger.error( e );
    }
}

const activemq = Config.getActiveMQConfig();
const client = new Stomp( activemq.host, activemq.port, activemq.username, activemq.password );
client.connect( (sessionId) => {

    Logger.info("connected to queue");
    client.subscribe( queueSimulation, (body, headers) => {
        console.log('incoming message:', body);
        parseMessage( body );
    });

});

