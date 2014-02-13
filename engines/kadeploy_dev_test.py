#!/usr/bin/env python
from execo import *
from execo_g5k import *
from execo_engine import *
from pprint import pformat



class kadeploy_dev_test(Engine):
    """A execo engine to perform test of various configuration for kadeploy-dev."""

    def create_paramsweeper(self):
        """Test all the sites, with or without a KaVLAN and for several env."""
        params = {
            "version": ['kadeploy3-dev', 'kadeploy3'],
            "site": get_g5k_sites(),
            "kavlan": [True, False],
            "n_nodes": [1, 5, 10],
            "env": ['wheezy-x64-base', 'wheezy-x64-prod', 'wheezy-x64-xen',
                '/home/lpouilloux/synced/environments/kvm-nocompression/kvm-1.5-nocompression.env',
                '/home/lpouilloux/synced/environments/vm5k/vm5k.env',]
                  
            }
        logger.info('Defining parameters: %s', pformat(params))
        combs = sweep(params)
        return ParamSweeper(self.result_dir + "/sweeper", combs)

    def run(self):
        sweeper = self.create_paramsweeper()

        while len(sweeper.get_remaining()) > 0:
            comb = sweeper.get_next()
            g5k_configuration['kadeploy3'] = comb['version']
            logger.info('Treating combination %s', pformat(comb))
            resources = ""
            if comb['kavlan']:
                resources += "{type='kavlan'}/vlan=1+"
            resources += "nodes="+str(comb['n_nodes'])
            sub = OarSubmission(resources = resources, job_type = 'deploy', walltime = "0:30:00" )
            logger.info('Performing reservation of %s on site %s', resources, comb['site'])
            jobs = oarsub( [ (sub, comb['site']) ])

            if jobs[0][0]:
                try:
                    logger.info('Waiting for job to start')
                    wait_oar_job_start(jobs[0][0], jobs[0][1])
                    hosts = get_oar_job_nodes(jobs[0][0], jobs[0][1])
                    logger.info('Deployment of %s', ' '.join( [host.address for host in hosts ]))
                    kavlan = get_oar_job_kavlan(jobs[0][0], jobs[0][1])
                    if kavlan:
                        logger.info('In kavlan %s', kavlan)
                    logger.setLevel('DEBUG')
                    if '.env' in comb['env']:
                        deployed, undeployed = deploy(Deployment(hosts, env_file = comb['env'],
                                    vlan = kavlan), out = True)
                    else:
                        deployed, undeployed = deploy(Deployment(hosts, env_name = comb['env'],
                                    vlan = kavlan), out = True)
                    logger.setLevel('INFO')
                finally:
                    oardel([(jobs[0][0], jobs[0][1])])

            if len(undeployed) == 0:
                logger.info('%s is OK', slugify(comb))
            elif len(deployed) == 0:
                logger.error('%s is KO', slugify(comb))
            else:
                logger.warning('%s encountered problems with some hosts', slugify(comb))

            sweeper.done(comb)
