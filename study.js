'use strict';

const Path = require('path');
const Fs = require("fs");
const Unzipper = require('unzipper');
const { spawn } = require('child_process')

const Config = require('./config');
const Logger = require('./logger');
const ConfigurationError = require("./ConfigurationError");

const Arendel = require('./arendel');
const arendel = new Arendel( Config.getAlfrescoConfig() );


const STUDY_STEP_MESHING = 'meshing';
const STUDY_STEP_SIMULATION = 'simulation';
const STUDY_STEP_POSTPROC = 'postproc';

const BG_TASK_STATUS_TODO        = "TODO";
const BG_TASK_STATUS_PENDING     = "PENDING";
const BG_TASK_STATUS_RUNNING     = "RUNNING";
const BG_TASK_STATUS_DONE        = "DONE";
const BG_TASK_STATUS_FAILED      = "FAILED";

// actual error messages that should mark the simulation as failed:
// "FOAM FATAL ERROR"
// "le calcul a divergé"
// "commande introuvable" means there is an internal error in the script
const REGEXP_SIMULATION_ERROR = /FOAM FATAL ERROR|a divergé|commande introuvable/;

const studies = {};

class StudyCache {

    static register ( study ) {
        studies[ study.nodeRef ] = study;
    }

    static unregister ( study ) {
        delete studies[ study.nodeRef ];
    }

    static exists ( study ) {
        return !!studies[ study.nodeRef ];
    }

    static get ( nodeRef ) {
        return studies[ nodeRef ];
    }
}


class Study {

    constructor ( nodeRef ) {
        this.nodeRef = nodeRef;
        this.nodeId = (nodeRef.match( /SpacesStore\/(.+)$/ ) || [])[1];
        this.path = Path.resolve( Config.getStudiesDir(), this.nodeId );
        this.sim = null;
        this.meshing = null;
        this.step = null;
        this.child = null;
        Logger.debug("studyNodeRef:", this.nodeRef );
        Logger.debug("studyNodeId:", this.nodeId );
        Logger.debug("studyPath:", this.path );
    }

    setup () {
        if ( !Fs.existsSync( this.path ) ) {
            Logger.info("Creating study directory:", this.path );
            Fs.mkdirSync( this.path, { recursive: true, mode: 0o755 } );
        }
    }

    cleanup () {
        if ( !this.path || !Fs.existsSync( this.path ) ) {
            return null;
        }
        Logger.info("Removing study directory:", this.path );
        return this.execute( 'rm', [
            { opt: '-rf' },                  // recursive, force
            { val: this.path },
        ], Path.resolve( Config.getStudiesDir() ) );
    }

    download ( nodeId, filename ) {
        return arendel.download( nodeId, filename ).promise;
    }

    async extract ( archive ) {
        var self = this;
        try {
            await Fs.createReadStream( archive )
                .pipe(Unzipper.Parse())
                .on('entry', function(entry) {
                    const fileName = entry.path;
                    const type = entry.type; // 'Directory' or 'File'
                    const size = entry.vars.uncompressedSize; // There is also compressedSize;
                    Logger.debug("Extracting", fileName, "type:", type, "size:", size);
                    // we need to flatten out the directory hierarchy
                    if ( type === 'File' ) {
                        var filename = Path.basename(fileName);
                        Logger.info("Extracting", filename);
                        entry.pipe(Fs.createWriteStream(self.path + "/" + filename));
                    } else {
                        entry.autodrain();
                    }
                })
                .promise();
            Logger.info("Extraction successful");
        }
        catch (err) {
            Logger.error(err);
            throw new Error("Extraction failed, reason: " + err.message);
        }
    }

    execute ( prgname, args, workDir ) {
        function interpolatePath ( p ) {
            return p.replace('{scriptDir}', scriptDir).replace('{studyDir}', studyDir);
        }
        var script = prgname;
        var scriptDir = Path.resolve( Config.getStudiesDir() );

        const cfg = Config.getAiretdConfig();
        if ( cfg.programs[ prgname ] ) {
            script =  Path.resolve( cfg.path, cfg.programs[ prgname ] );
            if ( !Fs.existsSync( script ) ) {
                throw new ConfigurationError("wrong configuration of program "+prgname+": script "+script+" not found");
            }
            scriptDir = Path.dirname( script );
        }
        const studyDir = this.path;

        var _args = [];

        // python scripts are going to be run by the python interpreter
        // shell scripts and other executables are going to be run as-is
        if ( script.endsWith('.py') ) {
            // this is a python script so we need to make the script the first argument
            // and replace the script with the interpreter
            _args.push( script );
            script = cfg.python;
        }

        // interpolate and copy all arguments
        args.forEach( (a) => {
            // if there is an option name, push it first in the stack of arguments for the command
            if ( a.opt != null ) {
                _args.push( a.opt );
            }
            var val = a.val;
            if ( val != null ) {
                // interpolate the argument if it's a string
                val = typeof val === 'string' ? interpolatePath( val ) : val;
                // and push it in the stack of arguments for the command
                _args.push( val );

                // validate few things if possible
                switch ( a.type ) {
                case 'd':
                case 'dir':
                    if ( !Fs.existsSync( val ) ) {
                        if ( !a.createIfMissing ) {
                            throw new ConfigurationError( val+" not found" );
                        }
                        Fs.mkdirSync( val, { recursive: true } );
                        if ( !Fs.existsSync( val ) ) {
                            throw new ConfigurationError( "failed to create directory "+val );
                        }
                    }
                    if ( !Fs.statSync( val ).isDirectory() ) {
                        throw new ConfigurationError( val+" is not a directory" );
                    }
                    break;

                case 'f':
                case 'file':
                    if ( !Fs.existsSync( val ) ) {
                        throw new ConfigurationError( val+" not found" );
                    }
                    if ( !Fs.statSync( val ).isFile() ) {
                        throw new ConfigurationError( val+" is not a file" );
                    }
                    break;

                default:
                    break;
                }
            }
        });

        // ChildProcess.execfile's options
        const options = {
            cwd:        interpolatePath( workDir ? workDir : this.path ),
            detached:   true,         // this forces the child to create its own process group
            shell:      false,        // don't spawn a shell, run the command directly
        };

        const self = this;
        return new Promise( (resolve, reject) => {

            var child = spawn( script, _args, options );
            child.on('error', (err) => {
                Logger.error( "failed to start "+prgname);
                reject({
                    code:   -127,
                    signal: null,
                    stdout: "",
                    stderr: err.message
                });
            });
            self.child = child;
            var stdout = "", stderr = "";
            if ( child.stdout ) {
                child.stdout.on('data', (data) => {
                    stdout += data.toString();
                    Logger.info(self.nodeId+" STDOUT: "+data.toString());
                } );
            }
            if ( child.stderr ) {
                child.stderr.on('data', (data) => {
                    stderr += data.toString();
                    Logger.info(self.nodeId+" STDERR: "+data.toString());
                });
            }
            child.on('close', (code, signal) => {
                // 0 means success, anything else means failure
                // result.stdout, result.stderr
                Logger.info("========================= "+prgname+" START ===============================");
                Logger.info("cmdline: "+ _args.join(" ") );
                if ( code ) {
                    Logger.info("Terminated with status code: "+code);
                }
                if ( signal ) {
                    Logger.info("Terminated by signal: "+signal);
                }
                if ( stdout ) {
                    Logger.info("------------------- stdout --------------------");
                    Logger.info(stdout);
                }
                if ( stderr ) {
                    Logger.info("------------------- stderr --------------------");
                    Logger.info(stderr);
                }
                Logger.info("========================= "+prgname+" END   ===============================");

                if ( code || signal ) {
                    if ( code ) {
                        stderr += "\nTerminated with status code: "+code+"\n";
                    }
                    if ( signal ) {
                        stderr += "\nTerminated by signal: "+signal+"\n";
                    }
                    reject({
                        code:   code,
                        signal: signal,
                        stdout: stdout,
                        stderr: stderr
                    });
                } else {
                    resolve({
                        stdout: stdout,
                        stderr: stderr
                    });
                }
            });
        });
    }

    compress ( stage, list ) {
        const filename = Path.resolve( Config.getStudiesDir(), `${this.nodeId}-${stage}.7z` );
        if ( Fs.existsSync( filename ) ) {
            // delete the old file if necessary
            Fs.unlinkSync( filename );
        }
        var args = [
            { opt: 'a' },                   // add to archive
            { opt: '-r' },                  // recursive
            { val: filename },
        ];
        if ( list ) {
            list.forEach( (p) => args.push(p) );
        } else {
            args.push({ val: '{studyDir}', type: 'd' });
        }
        return this.execute( '7z', args, Path.resolve( Config.getStudiesDir() ) )
            .then( () => filename ); // if successful, pass the filename to the next promise in the chain
    }

    uncompress ( stage ) {
        const filename = Path.resolve( Config.getStudiesDir(), `${this.nodeId}-${stage}.7z` );
        return this.execute( '7z', [
            { opt: 'x' },                   // extract with full paths
            { val: filename },
        ], Path.resolve( Config.getStudiesDir() ) );
    }

    updateMeshingTask ( m, stage, stdout, stderr ) {
        if ( stage ) { m.stage = stage; }
        return arendel.meshingTaskUpdate( this.nodeRef, m.status, m.stage, stdout, stderr );
    }

    startMeshing () {
        if ( StudyCache.exists( this ) ) {
            return Promise.reject( new Error("Study already under processing") );
        }
        let archive = Path.resolve( Config.getStudiesDir(), this.nodeId + '.zip' );
        var self = this;
        return arendel.claimMeshingTask( this.nodeRef )
            .then( (m) => {
                self.meshing = m;
                if ( m.status !== BG_TASK_STATUS_RUNNING ) {
                    throw new Error("Invalid meshing status: " + m.status);
                }
                self.step = STUDY_STEP_MESHING;
                StudyCache.register( self );
            })
            .then( () => this.cleanup() )
            .then( () => this.setup() )
            .then( () => this.updateMeshingTask( this.meshing, "download input folder" ) )
            .then( () => arendel.getInputFolder( this.nodeRef ) )
            .then( (nodeId) => self.download( nodeId, archive ) )
            .then( () => this.updateMeshingTask( this.meshing, "extraction" ) )
            .then( () => this.extract( archive ) )
            .then( () => this.updateMeshingTask( this.meshing, "meshing" ) )
            .then( () => self.execute( 'preproc', [
                    { opt: "-p_working",     val: '{studyDir}',                  type: 'd' },
                    { opt: "-p_config",      val: '{scriptDir}/computationDict', type: 'f' },
                    { opt: "-np_mesh",       val: 20       }, // TODO: this should be a parameter specified in the study itself
                    { opt: "-snappy_enable", val: "false"  }  // TODO: should this be a parameter specified in the study itself?
                ]) )
            .then( (result) => {
                // TODO: parse the output and check whether the meshing task was really successful or not
                self.meshing.stdout = result.stdout;
                self.meshing.stderr = result.stderr;
            })
            .then( () => self.updateMeshingTask( self.meshing, "compress" ) )
            .then( () => self.compress( STUDY_STEP_MESHING ) )
            /*
            .then( () => self.updateMeshingTask( self.meshing, "upload" ) )
            .then( () => {
                // TODO: upload the result of this step in the study's "cfd_model_meshing" directory
            })
            */
            .then( () => {
                self.meshing.status = BG_TASK_STATUS_DONE;
                self.meshing.stage = "done"
            })
            .catch( (errOrResult) => {
                // TODO: manage the failure somehow
                if ( errOrResult instanceof Error ) {
                    const error = errOrResult;
                    console.error( error );
                    self.meshing = {
                        status:     BG_TASK_STATUS_FAILED,
                        stderr:     error.message,
                        stdour:     ""
                    };
                } else {
                    // here we have also stdout and stderr
                    const result = errOrResult;
                    self.meshing.stdout = result.stdout;
                    self.meshing.stderr = result.stderr;
                    self.meshing.status = BG_TASK_STATUS_FAILED;
                    console.log("===== stdout =====\n", self.meshing.stdout );
                    console.log("===== stderr =====\n", self.meshing.stderr );
                }
            })
            .finally( () => {
                StudyCache.unregister( self );
                if ( self.meshing ) {
                    self.updateMeshingTask( self.meshing, null, self.meshing.stdout, self.meshing.stderr )
                        .then((m) => {
                            if ( m.status !== self.meshing.status ) {
                                throw new Error("Invalid meshing status: " + m.status);
                            }
                        })
                        .catch( (err) => Logger.error(err) );
                }
            });
    }

    abortMeshing () {
        var s = StudyCache.get( this.nodeRef );
        if ( s == null ) {
            return arendel.meshingTaskUpdate( this.nodeRef, BG_TASK_STATUS_FAILED );
        }
        if ( s.step !== STUDY_STEP_MESHING ) {
            return Promise.reject( new Error("Study is not running the meshing step") );
        }
        process.kill( -s.child.pid );
        return Promise.resolve( null );
    }

    setupSimulation () {
        return this.execute( 'bash', [
            { val: Path.resolve( Config.getBinDir(), 'setupSimulation.sh' ) },
        ], this.path );
    }

    updateSimulationTask ( sim, res, stage ) {
        if ( stage ) { res.stage = stage; }
        return arendel.simulationTaskUpdate( sim.nodeRef, res.status, res.stage, res.stdout, res.stderr );
    }

    startSimulation ( simNodeRef ) {
        if ( StudyCache.exists( this ) ) {
            return Promise.reject( new Error("Study already under processing") );
        }

        this.result = { status: BG_TASK_STATUS_RUNNING, stage: null, stdout: "", stderr: "", filename: null };

        var self = this;
        return arendel.claimSimulationTask( simNodeRef )
            .then( (s) => {
                if ( s.status !== BG_TASK_STATUS_RUNNING ) {
                    throw new Error("Invalid simulation status: " + s.status);
                }
                // we own the simulation now
                self.sim = s;
                self.step = STUDY_STEP_SIMULATION;
                StudyCache.register( self );
            })
            .then( () => self.cleanup() )
            .then( () => self.updateSimulationTask( self.sim, self.result, "uncompress" ) )
            // TODO: the result of the meshing phase may have been uploaded in Alfresco, so we may need to download it here
            .then( () => self.uncompress( STUDY_STEP_MESHING ) )
            //.then( () => self.updateSimulationTask( self.sim, self.result, "download" ) )
            // TODO/FIXME: this step is currently used to reduce the interval of time in the simulation to make it faster to debug
            // it may be used to setup the parameters of the simulation
            //.then( () => self.setupSimulation() )
            .then( () => self.updateSimulationTask( self.sim, self.result, "simulation" ) )
            .then( () => self.execute( 'simulation', [
                    { opt: "-p", val: '{studyDir}', type: 'd' },
                    { opt: "-e", val: self.nodeId   },
                    { opt: "-n", val: 30            },  // TODO: this should be a parameter specified in the study itself
                    { opt: "-s", val: "1.5"         },  // TODO: this should be a parameter specified in the study itself
                ]) )
            .then( (result) => {
                self.result = result;
                self.result.status = BG_TASK_STATUS_RUNNING;

                // check the simulation's log to see if it was really successful or not
                if ( result.stderr.match( REGEXP_SIMULATION_ERROR ) || result.stdout.match( REGEXP_SIMULATION_ERROR ) ) {
                    self.sim.status = BG_TASK_STATUS_FAILED;
                } else {
                    self.sim.status = BG_TASK_STATUS_DONE;
                }
            })
            .then( () => self.updateSimulationTask( self.sim, self.result, "compressing" ) )
            .then( () => self.compress( STUDY_STEP_SIMULATION ) )
	    /*
            .then( (filename) => {
                self.result.filename = filename;
                return self.updateSimulationTask( self.sim, self.result, "uploading" );
            })
            .then( () => arendel.upload( self.nodeId, `results-${self.sim.runId}.7z`, self.result.filename ) )
	    */
            .catch( (errOrResult) => {
                self.result.status = BG_TASK_STATUS_FAILED;
                if ( errOrResult instanceof Error ) {
                    const error = errOrResult;
                    console.error( error );
                    // save the error in the result that will be sent back to alfresco
                    self.result.stderr += ("\n"+error.message);
                } else {
                    const result = errOrResult;
                    self.result.stdout += ("\n"+result.stdout);
                    self.result.stderr += ("\n"+result.stderr);
                }
            })
            .finally( () => {
                StudyCache.unregister( self );
                // check that we successfully claimed the simulation
                if ( self.sim ) {
                    // yes, this means it's up to us to update its status
                    // determine the final state of the simulation
                    if ( self.result.status == BG_TASK_STATUS_RUNNING ) {
                        self.result.status = ( self.sim.status != BG_TASK_STATUS_RUNNING ? self.sim.status : BG_TASK_STATUS_FAILED );
                    }
                    self.updateSimulationTask( self.sim, self.result )
                    .then( (s) => {
                        if ( s.status !== self.result.status ) {
                            throw new Error("Invalid simulation status: " + s.status);
                        }
                    })
                    .catch( (err) => Logger.error(err) );
                }
            });
    }

    abortSimulation ( simNodeRef ) {
        var s = StudyCache.get( this.nodeRef );
        if ( s == null ) {
            return arendel.simulationTaskUpdate( simNodeRef, BG_TASK_STATUS_FAILED, null, "", "user aborted" );
        }
        if ( s.step !== STUDY_STEP_SIMULATION ) {
            return Promise.reject( new Error("Study is not running the simulation step") );
        }
        if ( s.sim.nodeRef !== simNodeRef ) {
            return arendel.simulationTaskUpdate( simNodeRef, BG_TASK_STATUS_FAILED, null, "", "user aborted" );
        }
        process.kill( -s.child.pid );
        return Promise.resolve( null );
    }

    updatePostprocTask ( res, stage ) {
        if ( stage ) { res.stage = stage; }
        return arendel.postprocTaskUpdate( this.nodeRef, res.status, res.stage, res.stdout, res.stderr );
    }

    postproc () {
        if ( StudyCache.exists( this ) ) {
            return Promise.reject( new Error("Study already under processing") );
        }
        this.setup();
        this.result = { status: BG_TASK_STATUS_RUNNING };

        let archive = Path.resolve( Config.getStudiesDir(), this.nodeId + '-postprocInputs.zip' );

        var self = this;
        return arendel.claimPostprocTask( this.nodeRef )
            .then( (pp) => {
                if ( pp.status !== BG_TASK_STATUS_RUNNING ) {
                    throw new Error("Invalid postproc status: " + pp.status);
                }
                // we own the postprocess task now
                self.pp = pp;
                self.step = STUDY_STEP_POSTPROC;
                self.result.status = BG_TASK_STATUS_RUNNING;
                StudyCache.register( self );
            })
            .then( () => self.cleanup() )
            .then( () => self.updatePostprocTask( self.result, "uncompress" ) )
            // TODO: the result of the sdimulation phase may have been uploaded in Alfresco, so we may need to download it here
            .then( () => self.uncompress( STUDY_STEP_SIMULATION ) )

			// download postproc input filess
            .then( () => arendel.getPostprocInputFolder( this.nodeRef ) )
            .then( (nodeId) => self.download( nodeId, archive ) )
            .then( () => this.updatePostprocTask( self.result, "extraction" ) )
            .then( () => this.extract( archive ) )

			// run the emicalc task
            .then( () => self.updatePostprocTask( self.result, "emiCalc" ) )
            .then( () => self.execute( 'emiCalc', [
                    { opt: '-p_input',  val: '{studyDir}',            type: 'd' },
                    { opt: '-p_output', val: '{studyDir}/emiCalc',    type: 'd', createIfMissing: true },
                ], '{scriptDir}' ) )
            .then( (result) => {
                self.result = result;
                self.result.status = BG_TASK_STATUS_RUNNING;
                // check the emicalc's log to see if it was really successful or not
                console.log("===== stdout =====\n", result.stdout );
                console.log("===== stderr =====\n", result.stderr );
                // check for error messages
				if ( result.stderr.match( /IndexError:/ ) ) {
	                throw new Error("emicalc failed.");
				}
            })

            .then( () => self.updatePostprocTask( self.result, "meanAndConcat" ) )
            .then( () => self.execute( 'meanAndConcat', [
                    { opt: '-p_working', val: '{studyDir}', type: 'd' },
                    { opt: '-p_output',  val: '{studyDir}/probes_treated', type: 'd', createIfMissing: true }
            ]) )
            .then( (result) => {
                self.result = result;
                self.result.status = BG_TASK_STATUS_RUNNING;
                // check the simulation's log to see if it was really successful or not
                // TODO: check for error messages
                console.log("===== stdout =====\n", result.stdout );
                console.log("===== stderr =====\n", result.stderr );

            })
            .then( () => self.updatePostprocTask( self.result, "probesMeanYear" ) )
            .then( () => self.execute( 'probesMeanYear', [
                    { opt: '-p_working', val: '{studyDir}',               type: 'd' },
                    { opt: '-p_probes_treated', val: '{studyDir}/probes_treated', type: 'd' },
                    { opt: '-p_freq',    val: '{studyDir}/frequencesVent',     type: 'f' },
                    { opt: '-p_sigmo',   val: '{studyDir}/parametresSigmoide', type: 'f' },
                    { opt: '-p_config',  val: '{scriptDir}/config',             type: 'f' },
                ]) )
            .then( (result) => {
                self.result = result;
                self.result.status = BG_TASK_STATUS_RUNNING;
                // check the simulation's log to see if it was really successful or not
                // TODO: check for error messages
                console.log("===== stdout =====\n", result.stdout );
                console.log("===== stderr =====\n", result.stderr );

            })
            .then( () => self.updatePostprocTask( self.result, "polluant" ) )
            .then( () => self.execute( 'polluant', [
                    { opt: '-p_scale',        val: '{studyDir}/settings_for_images',   type: 'f' },
                    { opt: '-p_logo',         val: '{scriptDir}/Logo_airetd.png',       type: 'f' },
                    { opt: '-p_treated_data', val: '{studyDir}/probes_treated',                 type: 'd' },
                ]) )
            .then( (result) => {
                self.result = result;
                self.result.status = BG_TASK_STATUS_RUNNING;
                // check the simulation's log to see if it was really successful or not
                // TODO: check for error messages
                console.log("===== stdout =====\n", result.stdout );
                console.log("===== stderr =====\n", result.stderr );

            })
            .then( () => self.updatePostprocTask( self.result, "compress" ) )
            .then( () => self.compress( STUDY_STEP_POSTPROC, [
                { val: '{studyDir}/emiCalc', type: 'd' },
                { val: '{studyDir}/probes_treated', type: 'd' }
            ] ) )
            .then( (filename) => {
                self.result.filename = filename;
                return self.updatePostprocTask( self.result, "uploading" );
            })
            .then( () => arendel.upload( self.nodeId, "final-results.7z", self.result.filename, "${cfd.postproc}", "cfd:postproc_result" ) )
            .then( () => {
                // if we get here without error it means the task is done
                if ( self.result.status === BG_TASK_STATUS_RUNNING ) {
                    self.result.status = BG_TASK_STATUS_DONE;
                }
            })
            .catch( (errOrResult) => {
                self.result.status = BG_TASK_STATUS_FAILED;
                if ( errOrResult instanceof Error ) {
                    const error = errOrResult;
                    console.error( error );
                    // save the error in the result that will be sent back to alfresco
                    self.result.stderr = error.message;
                    self.result.stdout = "";
                } else {
                    const result = errOrResult;
                    self.result.stdout = result.stdout;
                    self.result.stderr = result.stderr;
                }
            })
            .finally( () => {
                StudyCache.unregister( self );
                // check that we successfully claimed the simulation
                if ( self.pp ) {
                    // yes, this means it's up to us to update its status
                    self.updatePostprocTask( self.result )
                    .then( (pp) => {
                        if ( pp.status !== self.result.status ) {
                            throw new Error("Invalid postprocess status: " + pp.status);
                        }
                    })
                    .catch( (err) => Logger.error(err) );
                }
            });
    }

    abortPostproc () {
        var s = StudyCache.get( this.nodeRef );
        if ( s == null ) {
            return arendel.postprocTaskUpdate( this.nodeRef, BG_TASK_STATUS_FAILED, null, "", "user aborted" );
        }
        if ( s.step !== STUDY_STEP_POSTPROC ) {
            return Promise.reject( new Error("Study is not running the postprocessing step") );
        }
        process.kill( -s.child.pid );
        return Promise.resolve( null );
    }
}

Study.STEPS = {
    MESHING:    STUDY_STEP_MESHING,
    SIMULATION: STUDY_STEP_SIMULATION,
    POSTPROC:   STUDY_STEP_POSTPROC
};

module.exports = Study;
