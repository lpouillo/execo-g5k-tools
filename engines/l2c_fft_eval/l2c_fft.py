#!/usr/bin/env python

import os, math, sys

from pprint import pformat
from tempfile import mkstemp
from execo import Process
from execo import logger as ex_log
from execo.log import style
from execo_g5k import get_site_clusters, OarSubmission, oardel, get_cluster_site, \
    oarsub, wait_oar_job_start, get_oar_job_nodes, get_host_attributes, get_oar_job_info, \
    g5k_configuration
from execo_engine import Engine, logger, ParamSweeper, sweep, slugify, igeom


# Configure OAR to use always the same key
g5k_configuration['oar_job_key_file'] = '/home/jrichard/.oar_key'
#ex_log.setLevel('DEBUG')


def expRange(start, end, base=2):
    """
        Generate a list containing geometric progression
        starting from 'start' and ending by 'end'
    """
    return igeom(start, end, int(math.log(end/start)/math.log(base)+1.5))


class l2c_fft(Engine):
    workingPath = '/home/jrichard/l2c-fft-new-distrib/bin'
    genLadScript = '/home/jrichard/l2c-fft-new-distrib/src/utils/gen-lad/genPencil.py'

    def run(self):
        """
            Main engine method to perform the experiment
        """
        self.define_parameters()
        
        while len(self.sweeper.get_remaining()) > 0:
            # Getting the next combination
            comb = self.sweeper.get_next()
            logger.info(style.host(slugify(comb)) + ' has been started')
            self.get_nodes(comb)

            # If the job is broken, the program is stopped
            if get_oar_job_info(self.oar_job_id, self.frontend)['state'] == 'Error': 
                break

            try:
                self.workflow(comb)

                # Process all combinations that can use the same submission
                while True:
                    # Find the next combination combinations that can use the same submission
                    subcomb = self.sweeper.get_next(lambda r: 
                        filter(lambda x: x['cores'] == comb['cores']
                                        and x['cluster'] == comb['cluster'], r))

                    if not subcomb: 
                        logger.info('No more combination for cluster=%s and cores=%s',
                            comb['cluster'], comb['cores'])
                        break
                    else:
                        logger.info(style.host(slugify(subcomb)) + ' has been started')

                        if get_oar_job_info(self.oar_job_id, self.frontend)['state'] != 'Error':
                            self.workflow(subcomb)
                        else:
                            break
            
            # Whatever happens (errors, end of loop), the job is deleted
            finally:
                logger.info('Deleting job...')
                oardel([(self.oar_job_id, self.frontend)])

    def workflow(self, comb):
        """
            Compute one application launch 
            using a given parameter group
        """
        comb_ok = False
        try:
            # Generate configuration file needed by MPI processes
            logger.info("Generating assembly file...")
            py = comb['cores'] / comb['px']
            prepare = Process('cd %s && python %s %d %d %d %d %d %s app.lad' % 
                (self.workingPath, self.genLadScript, comb['datasize'], comb['datasize'], 
                    comb['datasize'], comb['px'], py, comb['transposition']))
            prepare.shell = True
            prepare.run()

            # Generate the MPI host file
            mfile = self.generate_machine_file()

            # Start L2C
            lad = "./app.lad"
            logger.info("Computing...")
            res = Process("export OAR_JOB_KEY_FILE=~/.oar_key ; cd %s && l2c_loader -M,-machinefile,%s --mpi -c %d %s" % (self.workingPath, mfile, comb['cores'], lad))
            res.shell = True
            res.stdout_handlers.append(os.path.join(self.result_dir, slugify(comb) + '.out'))
            res.stdout_handlers.append(sys.stdout)
            res.stderr_handlers.append(os.path.join(self.result_dir, slugify(comb) + '.err'))
            res.stderr_handlers.append(sys.stderr)
            res.run()
            if not res.ok:
                logger.error('Bad L2C termination')
                raise Exception('Bad L2C termination')
            if len(res.stderr) > 0: # WARNING: when L2C cannot find the LAD file or something strange like this
                logger.warning('Not empty error output')

            # Clean configuration files
            logger.info("Removing assembly files...")
            res = Process('cd %s && rm -f app.lad*' % self.workingPath)
            res.shell = True
            res.run()
                
            comb_ok = True
        except Exception:
            pass
        finally:
            if comb_ok:
                self.sweeper.done(comb)
                logger.info(style.host(slugify(comb)) + ' has been done')
            else:
                self.sweeper.cancel(comb)
                logger.warning(style.host(slugify(comb)) + ' has been canceled')
        
            logger.info(style.step('%s Remaining'),
                        len(self.sweeper.get_remaining()))

    def define_parameters(self):
        """
            Define the parametters used by the L2C application
        """
        parameters = {
            'cluster': [cluster for site in ['grenoble', 'nancy'] 
                        for cluster in get_site_clusters(site) if cluster != 'graphite'],
            'cores': {i: {'px': expRange(1, i)} 
                      for i in expRange(4, 64)},
            'datasize': expRange(256, 256),
            'transposition': ['XYZ', 'XZY', 'YXZ', 'YZX', 'ZXY', 'ZYX']}
        
        logger.info(pformat(parameters))
        sweeps = sweep(parameters)
        self.sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"), sweeps)        
        logger.info('Number of parameters combinations %s', len(self.sweeper.get_remaining()))

    def get_nodes(self, comb):
        """
            Perform a submission for a given comb and 
            retrieve the submission node list
        """
        logger.info('Performing submission')
        n_core = get_host_attributes(comb['cluster'] + '-1')['architecture']['smt_size']
        submission = OarSubmission(resources="nodes=%d" % (max(1, comb['cores']/n_core), ), 
                   sql_properties="cluster='%s'"%comb['cluster'],
                   job_type="besteffort", 
                   name="l2c_fft_eval")
        self.oar_job_id, self.frontend = oarsub([(submission, get_cluster_site(comb['cluster']))])[0]
        logger.info("Waiting for job start")
        wait_oar_job_start(self.oar_job_id, self.frontend)
        logger.info("Retrieving hosts list")
        nodes = get_oar_job_nodes(self.oar_job_id, self.frontend)
        self.hosts = [host for host in nodes for i in range(n_core)]

    def generate_machine_file(self):
        """
            Generate a machine file used by MPI 
            to know which nodes use during the computation
        """
        fd, mfile = mkstemp(dir='/tmp/', prefix='mfile_')
        f = os.fdopen(fd, 'w')
        f.write('\n'.join([host.address for host in self.hosts]))
        f.close()
        return mfile


if __name__ == "__main__":
    engine = l2c_fft()
    engine.start()    

