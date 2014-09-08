#!/usr/bin/env python
from execo import *
from execo_g5k import *
from execo_engine import *
from pprint import pformat


class kadeploy_trace(Engine):
    """A execo engine to record the trace of Kadeploy in various conditions"""

    def create_paramsweeper(self):
        """Test all the clusters for several env."""
        params = {
            "cluster": ['taurus'],
            "cluster": get_g5k_clusters(),
            "env": ['wheezy-x64-min', 'wheezy-x64-prod', 'wheezy-x64-big']
            }
        logger.info('Defining parameters: %s', pformat(params))

        combs = sweep(params)
        return ParamSweeper(self.result_dir + "/sweeper", combs)

    def run(self):
        sweeper = self.create_paramsweeper()

        while len(sweeper.get_remaining()) > 0:
            comb = sweeper.get_next()
            logger.info('Treating combination %s', pformat(comb))
            cluster = comb['cluster']
            site = get_cluster_site(cluster)
            
            sub = OarSubmission(resources="{cluster in ('" + cluster +"')}nodes=1",
                                job_type='deploy',
                                walltime="0:30:00",
                                name="kadeploy_trace")
            logger.info('Performing reservation of 1 node on cluster %s', cluster)
            jobs = oarsub([(sub, site)])

            if jobs[0][0]:
                try:
                    logger.info('Waiting for job to start')
                    wait_oar_job_start(jobs[0][0], jobs[0][1])
                    hosts = get_oar_job_nodes(jobs[0][0], jobs[0][1])
                    logger.info('START deployment env-%s-cluster-%s', comb['env'], 
                        cluster)
                    deployed, undeployed = deploy(Deployment(hosts, env_name=comb['env']), 
                                                  out=True)
                    logger.info('END deployment')
                finally:
                    oardel([(jobs[0][0], jobs[0][1])])
            else:
                continue

            if len(undeployed) == 0:
                logger.info('%s is OK', slugify(comb))
            elif len(deployed) == 0:
                logger.error('%s is KO', slugify(comb))
            else:
                logger.warning('%s encountered problems with some hosts', slugify(comb))

            sweeper.done(comb)


if __name__ == "__main__":
    e = kadeploy_trace()
    e.start()
    