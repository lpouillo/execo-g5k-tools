#!/usr/bin/env python
import os
from itertools import takewhile, count
from execo import SshProcess, Remote, Put, format_date
from execo_g5k import oarsub, oardel, OarSubmission, \
    get_oar_job_nodes, wait_oar_job_start, \
    get_host_attributes, get_cluster_site, get_host_site
from execo_engine import Engine, ParamSweeper, sweep, \
    slugify, logger

def pred_cb(ts):
    logger.info("job start prediction = %s" % (format_date(ts),))

def get_mpi_opts(cluster):
    # MPI configuration depends on the cluster
    # see https://www.grid5000.fr/mediawiki/index.php/FAQ#MPI_options_to_use
    if cluster in ['parapluie', 'parapide', 'griffon',
                   'graphene', 'edel', 'adonis', 'genepi' ]:
        mpi_opts = '--mca btl openib,sm,self --mca pml ^cm'
    elif cluster in ['suno', 'chinqchint']:
        mpi_opts = '--mca pml ob1 --mca btl tcp,self'
    elif cluster in ['sol']:
        mpi_opts = '--mca pml cm'
    else:
        mpi_opts = '--mca pml ob1 --mca btl tcp,self'
    return mpi_opts

class mpi_bench(Engine):

    def run(self):
        """Inherited method, put here the code for running the engine"""
        self.define_parameters()
        if self.prepare_bench():
            logger.info('Bench prepared on all frontends')
            self.run_xp()

    def define_parameters(self):
        """Create the iterator on the parameters combinations to be explored"""
        # fixed number of nodes
        self.n_nodes = 4
        # choose a list of clusters
        clusters = ['graphene', 'petitprince', 'edel', 'paradent', 'stremi']
        #clusters = ['petitprince', 'paradent']
        # compute the maximum number of cores among all clusters
        max_core = self.n_nodes * max([
                get_host_attributes(cluster + '-1')['architecture']['smt_size']
                for cluster in clusters])
        # define the parameters
        self.parameters = {
            'cluster' : clusters,
            'n_core': filter(lambda i: i >= self.n_nodes,
                             list(takewhile(lambda i: i<max_core,
                                            (2**i for i in count(0, 1))))),
            'size' : ['A', 'B', 'C']
            }
        logger.info(self.parameters)
        # define the iterator over the parameters combinations
        self.sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"),
                                    sweep(self.parameters))
        logger.info('Number of parameters combinations %s' % len(self.sweeper.get_remaining()))

    def prepare_bench(self):
        """bench configuration and compilation, copy binaries to frontends
        
        return True if preparation is ok
        """
        logger.info("preparation: configure and compile benchmark")
        # the involved sites. We will do the compilation on the first of these.
        sites = list(set(map(get_cluster_site, self.parameters['cluster'])))
        # generate the bench compilation configuration
        bench_list = '\n'.join([ 'lu\t%s\t%s' % (size, n_core)
                                 for n_core in self.parameters['n_core']
                                 for size in self.parameters['size'] ])
        # Reserving a node because compiling on the frontend is forbidden
        # and because we need mpif77
        jobs = oarsub([(OarSubmission(resources = "nodes=1",
                                      job_type = 'allow_classic_ssh',
                                      walltime ='0:10:00'), sites[0])])
        if jobs[0][0]:
            try:
                logger.info("copying bench archive to %s" % (sites[0],))
                copy_bench = Put([sites[0]], ['NPB3.3-MPI.tar.bz2']).run()
                logger.info("extracting bench archive on %s" % (sites[0],))
                extract_bench = Remote('tar -xjf NPB3.3-MPI.tar.bz2', [sites[0]]).run()
                logger.info("waiting job start %s" % (jobs[0],))
                wait_oar_job_start(*jobs[0], prediction_callback = pred_cb)
                logger.info("getting nodes of %s" % (jobs[0],))
                nodes = get_oar_job_nodes(*jobs[0])
                logger.info("configure bench compilation")
                conf_bench = Remote('echo "%s" > ~/NPB3.3-MPI/config/suite.def' % bench_list, nodes).run()
                logger.info("compil bench")
                compilation = Remote('cd NPB3.3-MPI && make clean && make suite', nodes).run()
                logger.info("compil finished")
            except:
                logger.error("unable to compile bench")
                return False
            finally:
                oardel(jobs)
        # Copying binaries to all other frontends
        frontends = sites[1:]
        rsync = Remote('rsync -avuP ~/NPB3.3-MPI/ {{frontends}}:NPB3.3-MPI', 
                       [get_host_site(nodes[0])] * len(frontends)) 
        rsync.run()
        return compilation.ok and rsync.ok

    def run_xp(self):
        """Iterate over the parameters and execute the bench"""
        while len(self.sweeper.get_remaining()) > 0:
            comb = self.sweeper.get_next()
            if comb['n_core'] > get_host_attributes(comb['cluster']+'-1')['architecture']['smt_size'] * self.n_nodes: 
                self.sweeper.skip(comb)
                continue
            logger.info('Processing new combination %s' % (comb,))
            site = get_cluster_site(comb['cluster'])
            jobs = oarsub([(OarSubmission(resources = "{cluster='" + comb['cluster']+"'}/nodes=" + str(self.n_nodes),
                                          job_type = 'allow_classic_ssh', 
                                          walltime ='0:10:00'), 
                            site)])
            if jobs[0][0]:
                try:
                    wait_oar_job_start(*jobs[0])
                    nodes = get_oar_job_nodes(*jobs[0])
                    bench_cmd = 'mpirun -H %s -n %i %s ~/NPB3.3-MPI/bin/lu.%s.%i' % (
                        ",".join([node.address for node in nodes]),
                        comb['n_core'],
                        get_mpi_opts(comb['cluster']),
                        comb['size'],
                        comb['n_core'])
                    lu_bench = SshProcess(bench_cmd, nodes[0])
                    lu_bench.stdout_handlers.append(self.result_dir + '/' + slugify(comb) + '.out')
                    lu_bench.run()
                    if lu_bench.ok:
                        logger.info("comb ok: %s" % (comb,))
                        self.sweeper.done(comb)
                        continue
                finally:
                    oardel(jobs)
            logger.info("comb NOT ok: %s" % (comb,))
            self.sweeper.cancel(comb)

if __name__ == "__main__":
    engine = mpi_bench()
    engine.start()
