'use strict';

const Alfresco = require('./alfresco');
const Logger = require('./logger');

const URI_ARENDEL_API = '/alfresco/s/arendel';


function axiosErrorHandler ( error ) {

    console.log( error.config );

    if ( error.response ) {
        // The request was made and the server responded with a status code
        // that falls out of the range of 2xx
        var response = error.response;

        console.log( response.status );
        console.log( response.headers );
        console.log( response.data );

        switch ( response.status ) {
        case 400:
            throw new Error("Invalid parameter");
        case 401:
            throw new Error("Authentication failed");
        case 403:
            throw new Error("Permission denied");
        default:
            throw new Error("Unexected error (code: " + response.status + ")" );
        }

    } else if ( error.request ) {
        // The request was made but no response was received
        // `error.request` is an instance of XMLHttpRequest in the browser and an instance of
        // http.ClientRequest in node.js
        console.log( error.request );
        throw new Error("No answer from server" );

    } else {
        // Something happened in setting up the request that triggered an Error
        console.log('Error', error.message);
        throw new Error("Client internal error" );
    }
}


function processArendelApiResponse ( response ) {
    var scriptResponse = response.data;
    Logger.debug( "API response:", JSON.stringify( scriptResponse, null, 4 ) );
    /*
     * the Arendel API normally returns the following status codes:
     * 200 OK
     * 400 Bad request
     * 404 Not found
     * 500 Server error
     */
    if ( scriptResponse.statusCodeValue !== 200 ) {
        throw new Error("Arendel API error: " + scriptResponse.statusCode );
    }
    return scriptResponse.body;
}


class Arendel extends Alfresco {

    getInputFolder ( nodeRef ) {
        return this.getChildren( nodeRef, "(nodeType='cfd:inputs')" ).then( (nodes) => {
            if ( !nodes || !nodes.length ) {
                throw new Error("Input folder not found");
            }
            var folder = nodes[0].entry;
            console.dir( folder );
            return folder.id;
        });
    }

    getPreliminaryStudyFolder ( nodeRef ) {
        return this.getChildren( nodeRef, "(nodeType='cfd:preliminary_study')" ).then( (nodes) => {
            if ( !nodes || !nodes.length ) {
                throw new Error("Preliminary study not found");
            }
            var folder = nodes[0].entry;
            console.dir( folder );
            return folder.id;
        });
    }

    /**
     * @params {string} nodeRef - the study's nodeRef
     */
    claimMeshingTask ( nodeRef ) {
        return this.conn.get( URI_ARENDEL_API + '/meshing/claim', { params: { nodeRef: nodeRef } } )
                        .then( processArendelApiResponse, axiosErrorHandler )
                        .then( (m) => {
                            return { nodeRef: m.nodeRef, status: m.cfd_meshing_status };
                        });
    }

    /**
     * @params {string} nodeRef - the study's nodeRef
     */
    meshingTaskUpdate ( nodeRef, status, stage, stdout, stderr ) {
        const payload = {
            status:     status,
            stage:      stage,
            stdout:     stdout,
            stderr:     stderr,
        };
        return this.conn.post( URI_ARENDEL_API + '/meshing/update', payload, { params: { nodeRef: nodeRef } } )
                        .then( processArendelApiResponse, axiosErrorHandler )
                        .then( (m) => {
                            return { nodeRef: m.nodeRef, status: m.cfd_meshing_status };
                        });
    }

    /**
     * @params {string} nodeRef - the simulation's nodeRef
     */
    claimSimulationTask ( nodeRef ) {
        const payload = {
                cfd_start_ts:       (new Date()).valueOf()
            };
        return this.conn.post( URI_ARENDEL_API + '/simulation/claim', payload, { params: { nodeRef: nodeRef } } )
                        .then( processArendelApiResponse, axiosErrorHandler )
                        .then( (s) => {
                            return {
                                nodeRef:        s.nodeRef,
                                status:         s.cfd_simulation_status,
                                runId:          s.cfd_runid,
                            }
                        });
    }

    /**
     * @params {string} nodeRef - the simulation's nodeRef
     */
    simulationTaskUpdate ( nodeRef, status, stage, stdout, stderr ) {
        const payload = {
                status:     status,
                stage:      stage,
                ts:         (new Date()).valueOf(),
                stdout:     stdout,
                stderr:     stderr
            };
        return this.conn.post( URI_ARENDEL_API + '/simulation/update', payload, { params: { nodeRef: nodeRef } } )
                        .then( processArendelApiResponse, axiosErrorHandler )
                        .then( (s) => {
                            return { nodeRef: nodeRef, status: s.cfd_simulation_status };
                        });
    }

    /**
     * @params {string} nodeRef - the simulation's nodeRef
     */
    claimPostprocTask ( nodeRef ) {
        return this.conn.get( URI_ARENDEL_API + '/postproc/claim', { params: { nodeRef: nodeRef } } )
            .then( processArendelApiResponse, axiosErrorHandler )
            .then( (pp) => {
                return { nodeRef: pp.nodeRef, status: pp.cfd_postproc_status };
            });
    }

    getPostprocInputFolder ( nodeRef ) {
        return this.getChildren( nodeRef, "(nodeType='cfd:postproc_inputs')" ).then( (nodes) => {
            if ( !nodes || !nodes.length ) {
                throw new Error("Postproc input folder not found");
            }
            var folder = nodes[0].entry;
            console.dir( folder );
            return folder.id;
        });
    }

    /**
     * @params {string} nodeRef - the simulation's nodeRef
     */
    postprocTaskUpdate ( nodeRef, status, stage, stdout, stderr ) {
        const payload = {
                status:     status,
                stage:      stage,
                ts:         (new Date()).valueOf(),
                stdout:     stdout,
                stderr:     stderr
            };
        return this.conn.post( URI_ARENDEL_API + '/postproc/update', payload, { params: { nodeRef: nodeRef } } )
                        .then( processArendelApiResponse, axiosErrorHandler )
                        .then( (pp) => {
                            return { nodeRef: pp.nodeRef, status: pp.cfd_postproc_status };
                        });
    }

}

module.exports = Arendel;
