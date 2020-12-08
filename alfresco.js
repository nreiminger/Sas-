'use strict';

const Fs = require('fs');
const Axios = require('axios');
const util = require('util');
const setTimeoutPromise = util.promisify(setTimeout);

const logger = require('./logger');


// this is in milliseconds
const POLLING_PERIOD = 10000;
// use chunks of 8MB
const CHUNK_SIZE = 8 * 1024 * 1024;

function alfrescoApiErrorHandler ( error ) {

    //console.log( error.config );

    if ( error.response ) {
        // The request was made and the server responded with a status code
        // that falls out of the range of 2xx
        var response = error.response;

        console.log( response.status );
        console.log( response.headers );
        console.log( response.data );
        console.dir( error.request );

        // alfresco's API is expected to return only one of the following errors:
        // 400 Invalid parameter
        // 401 Authentication failed
        // 403 Permission denied
        // default unexpected error
        //console.dir(response);
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

    } else if ( error.isAxiosError ) {
        console.error( "Internal error:", error.message, error.stack );
        throw new Error( error.message );

    } else if ( error.request ) {
        // The request was made but no response was received
        // `error.request` is an instance of XMLHttpRequest in the browser and an instance of
        // http.ClientRequest in node.js
        console.dir( error.request );
        throw new Error("No answer from server");

    } else {
        // Something happened in setting up the request that triggered an Error
        console.log('Error', error.message);
        throw new Error("Client internal error" );
    }
}


// NOTE: it seems there is a new public API for downloads:
//   https://api-explorer.alfresco.com/api-explorer/#!/downloads/createDownload
// it works mostly like the internal one but it's a bit simpler

// download statuses: "PENDING", "CANCELLED", "IN_PROGRESS", "DONE", "MAX_CONTENT_SIZE_EXCEEDED"

const URI_ALFRESCO_API = '/alfresco/api/-default-/public/alfresco/versions/1';
const URI_CMIS11_API = '/alfresco/api/-default-/public/cmis/versions/1.1';

const NODEREF_REGEXP = /SpacesStore\/(.+)$/;

class Download {

    constructor ( conn, folderNodeId, filename ) {
        this.conn = conn;
        this.filename = filename;
        this.nodeId = null;

        var self = this;

        logger.debug("Creating the download object");
        this.promise = this.conn.post( URI_ALFRESCO_API + "/downloads", { nodeIds: [ folderNodeId ] } ).then( (response) => {
                /* example of response.data:
                 * {
                 *   "entry": {
                 *     "filesAdded": 0,
                 *     "bytesAdded": 0,
                 *     "id": "string",
                 *     "totalFiles": 0,
                 *     "totalBytes": 0,
                 *     "status": "PENDING"
                 *   }
                 * }
                 */
                self.nodeId = response.data.entry.id;
                self.status = response.data.entry.status;
                logger.debug("download nodeId:", self.nodeId );
                return self.waitUntilReady();

            }, alfrescoApiErrorHandler )
            .catch( function( error ) {
                throw new Error("Download failed, reason: " + error.message );
            });
    }

    waitUntilReady () {
        var self = this;
        switch( this.status ) {
        case 'PENDING':
        case 'IN_PROGRESS':
            // these are equivalent because they both mean we need to wait for while and try again
            return setTimeoutPromise( POLLING_PERIOD ).then( () => {
                logger.debug("polling status of nodeId:", self.nodeId );
                return self.conn.get( URI_ALFRESCO_API + "/downloads/" + self.nodeId ).then( function(response) {
                    /*
                     * response.data looks like:
                     * {
                     *   "entry": {
                     *     "filesAdded":5,
                     *     "bytesAdded":957320,
                     *     "totalBytes":957320,
                     *     "id":"22465bbc-9cd4-49e6-b3b1-24efdfa4f91a",
                     *     "totalFiles":5,
                     *     "status":"DONE"
                     *   }
                     * }
                     */
                    self.status = response.data.entry.status;
                    return self.waitUntilReady();

                }, alfrescoApiErrorHandler )
                .catch( function( error ) {
                    throw new Error("Failed to check download status, reason: " + error.message );
                });
            });

        case 'DONE':
            // ok, the download is ready so we can proceeed
            const writer = Fs.createWriteStream( this.filename );

            logger.debug("starting download of nodeId:", this.nodeId );

            return this.conn.get( URI_ALFRESCO_API + "/nodes/" + this.nodeId + "/content?attachment=true", { responseType: 'stream' } ).then( (response) => {
                    // write the received zip file somewhere
                    response.data.pipe( writer );
                    return new Promise((resolve, reject) => {
                        writer.on('finish', resolve)
                        writer.on('error', reject)

                    }).then( () => {
                        logger.debug("Completed download of nodeId:", self.nodeId );
                        // return the study
                        return self.study;

                    }, (err) => {
                        logger.error( err );
                        throw new Error("Failed to download zip file");
                    });

                }, alfrescoApiErrorHandler )
                .catch( function( error ) {
                    throw new Error("Failed to download zip file, reason: " + error.message );
                });

        case 'CANCELLED':
            // the user stopped the download somehow. It should never happen because we created this download request, not the user
            break;

        case 'MAX_CONTENT_SIZE_EXCEEDED':
            // this is a blocking error, we cannot continue
            break;

        default:
            // this is an internal error, it should never happen accordingly to alfresco's API documentation
            break;
        }
    }

}


class Alfresco {

    constructor ( config ) {

        this.conn = Axios.create({
            baseURL:    config.url,
            timeout:    30000,
            auth:       {
                username:   config.username,
                password:   config.password,
            },
            headers: {
            },
            maxContentLength: Infinity,
            maxBodyLength: Infinity
        });

    }

    download ( folderNodeId, filename ) {
        return new Download( this.conn, folderNodeId, filename );
    }

    getChildren ( nodeRef, where, options ) {
        var nodeId = (nodeRef.match( NODEREF_REGEXP ) || [])[1];
        //  /nodes/${nodeId}/children
        // { skipCount: 0, maxItems: 100, where: "(nodeType='cfd:inputs')" }
        var opts = options || {};
        var params = {
                skipCount:  opts.skipCount || 0,
                maxItems:   opts.maxItems  || 100,
                where:      where
            };
        return this.conn.get( URI_ALFRESCO_API + "/nodes/" + nodeId + "/children", { params: params }).then( (response) => {
            if ( response.data.error ) {
                var err = response.data.error;
                throw new Error( err.errorKey + "\n" + err.briefSummary );
            }
            return response.data.list.entries;
        });
    }

    createDocument ( studyNodeId, options ) {

        var opts = options || {};

        return this.conn.post( URI_ALFRESCO_API + "/nodes/" + studyNodeId + "/children", {
            name:           opts.name    || "results.7z",
            nodeType:       opts.type    || "cm:content",
            relativePath:   opts.relativePath || "cfd_simulations_data_root",
            properties:     {
                "cm:title":         opts.title       || "Simulation results",
                "cm:description":   opts.description || "Simulation results"
            }
        }, { params: { overwrite: true, autoRename: true } })
            .then( (response) => {
                console.log("created new document:", response.data.entry );
                return response.data.entry;
            })
            .catch( alfrescoApiErrorHandler );
    }

    upload ( studyNodeId, name, filename, relativePath, type ) {
        var self = this;
        return new Promise( ( resolve, reject ) => {
            try {
                var fileSize = Fs.statSync( filename ).size;
                var numChunks = Math.floor( fileSize / CHUNK_SIZE ) + ( fileSize % CHUNK_SIZE !== 0 ? 1 : 0 );
                logger.log("File:", filename, "size:", fileSize, "chunks:", numChunks);
                var counter = 0;
                var buffer = Buffer.allocUnsafe( CHUNK_SIZE );
                var uploadId = null;

                function uploadChunk ( fd ) {
                    Fs.read( fd, buffer, 0, CHUNK_SIZE, null, (err, bytesRead, buffer) => {
                        if ( err ) {
                            Fs.closeSync(fd);
                            reject( err );
                            return;
                        }
                        console.log("read", bytesRead, "bytes from file for chunk:", counter);
                        // just to be sure...
                        var chunk = buffer.slice(0, bytesRead);
                        var isLastChunk = ( counter >= numChunks );
                        logger.log("uploading chunk:", counter, "isLastChunk:", isLastChunk);
                        // append the chunk to the existing upload
                        self.conn.put( URI_CMIS11_API + '/atom/content', chunk, {
                            params: {
                                id:          uploadId,
                                append:      true,
                                isLastChunk: isLastChunk
                            },
                            headers: {
                                "Content-Type": "application/octet-stream",
                            }
                        })
                        .then( (response) => {
                            counter++;
                            logger.log("chunk", counter, "uploaded successfully");
                            if ( isLastChunk ) {
                                Fs.closeSync(fd);
                                resolve();
                                return;
                            }
                            uploadChunk(fd);
                        })
                        .catch( alfrescoApiErrorHandler )
                        .catch( (error) => {
                            Fs.closeSync(fd);
                            reject( error );
                        });
                    });
                }

                // try to open the file
                Fs.open( filename, (err, fd) => {
                    if ( err ) { return reject( err ); }

                    // we need to create an empty document first
                    self.createDocument( studyNodeId, { name: name, relativePath: relativePath, type: type }).then( (upload) => {
                        // start the upload
                        uploadId = upload.id;
                        uploadChunk( fd );
                    })
                    .catch( alfrescoApiErrorHandler )
                    .catch( (error) => {
                        Fs.closeSync(fd);
                        reject( error );
                    });
                });

            } catch (e) {
                reject(e);
            }

        });
    }
}


module.exports = Alfresco;
