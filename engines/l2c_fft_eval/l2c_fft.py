#!/usr/bin/env python

import os, math

from pprint import pformat
from tempfile import mkstemp
from execo import Process
from execo.log import style
from execo_g5k import get_site_clusters, OarSubmission, oardel, get_cluster_site, \
    oarsub, wait_oar_job_start, get_oar_job_nodes, get_host_attributes, get_oar_job_info, \
    g5k_configuration
from execo_engine import Engine, logger, ParamSweeper, sweep, slugify, igeom

g5k_configuration['oar_job_key_file'] = '/home/jrichard/.ssh/id_rsa'


class l2c_fft(Engine):
    
    workingPath = '/home/jrichard/l2c-fft-new-distrib'
    genLadScript = '/home/jrichard/l2c-fft-new-distrib/src/utils/gen-lad/genPencil.py'
    
    def run(self):
        """Main engine method to perform the experiment """
        self.define_parameters()
        
        while len(self.sweeper.get_remaining()) > 0:
            # Getting the next combination
            comb = self.sweeper.get_next()
            logger.info(slugify(comb) + ' has been started')
            self.get_nodes(comb)
            
            if get_oar_job_info(self.oar_job_id, self.frontend)['state'] == 'Error': 
                break
            try:
                self.workflow(comb)

                while True:
                    subcomb = self.sweeper.get_next(filter=lambda x: x['cores'] == comb['cores']
                                                    and x['cluster'] == comb['cluster'])
                    logger.info(slugify(subcomb) + ' has been started')
                    if not subcomb: 
                        break
                    else:
                        if get_oar_job_info(self.oar_job_id, self.frontend)['state'] != 'Error':
                            self.workflow(comb)
                        else:
                            break
            
            finally:
                logger.info('Deleting job')
#                 oardel([(self.oar_job_id, self.frontend)])
                    
            
                
            
    def workflow(self, comb):
        comb_ok = False
        try:
            py = comb['cores'] / comb['px']
            prepare = Process('cd %s && python %s %d %d %d %d %d %s app.lad' % 
                (self.workingPath, self.genLadScript, comb['datasize'], comb['datasize'], 
                    comb['datasize'], comb['px'], py, comb['transposition']))
            prepare.shell = True
            prepare.run()
            mfile = self.generate_machine_file()
                
            lad = "./n%d-p%dx%d-%s.lad" % (comb['datasize'], comb['px'], py, comb['transposition'])
            res = Process("bash -c \"cd %s && l2c_loader -M,-machinefile,%s --mpi -c %d %s\"" % (self.workingPath, mfile, comb['cores'], lad))
            res.shell = True
            res.stdout_handlers.append(self.result_dir + slugify(comb))
            res.run()
            if not res.ok:
                print res.stderr
                exit()
                
            rm_lad = Process('cd %s && rm -f app.lad*' % self.workingPath).run()
                
            comb_ok = True
        finally:
            if comb_ok:
                self.sweeper.done(comb)
                logger.info(slugify(comb) + ' has been done')
            else:
                self.sweeper.cancel(comb)
                logger.warning(slugify(comb) + ' has been canceled')
        
        logger.info(style.step('%s Remaining'),
                        len(self.sweeper.get_remaining()))
        
    
    def define_parameters(self):
        """ """
    
        parameters = {
            'cluster': [cluster for site in ['grenoble', 'nancy'] 
                        for cluster in get_site_clusters(site) if cluster != 'graphite'],
            'cores': {i: {'px':  igeom(1, i, int(math.log(i)/math.log(2)) + 1)} 
                      for i in igeom(4, 64, 5)},
            'datasize': igeom(256, 256, 1),
            'transposition': ['XYZ', 'XZY', 'YXZ', 'YZX', 'ZXY', 'ZYX']}
        
        logger.info(pformat(parameters))
        sweeps = sweep(parameters)
        self.sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"), sweeps)        
        logger.info('Number of parameters combinations %s', len(self.sweeper.get_remaining()))
        
    
    def get_nodes(self, comb):
        """ """
        logger.info('Performing submission')
        n_core = get_host_attributes(comb['cluster'] + '-1')['architecture']['smt_size']
        submission = OarSubmission(resources="nodes=%s"%(comb['cores']/n_core, ), 
                   sql_properties="cluster='%s'"%comb['cluster'],
                   name="l2c_fft_eval")
#                    job_type="allow_classic_ssh",
                   
        self.oar_job_id, self.frontend = oarsub([(submission, get_cluster_site(comb['cluster']))])[0]
        logger.info("Waiting for job start")
        wait_oar_job_start(self.oar_job_id, self.frontend)
        logger.info("Retrieving hosts list")
        nodes = get_oar_job_nodes(self.oar_job_id, self.frontend)
        self.hosts = [host for host in nodes 
            for i in range(n_core)]
        
        
    def generate_machine_file(self):
        """ """
        fd, mfile = mkstemp(dir='/tmp/', prefix='mfile_')
        f = os.fdopen(fd, 'w')
        f.write('\n'.join([host.address for host in self.hosts]))
        f.close()
        return mfile
        
if __name__ == "__main__":
    engine = l2c_fft()
    engine.start()    
    