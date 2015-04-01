#!/usr/bin/env python
from execo import SshProcess
from execo_g5k import g5k_configuration, OarSubmission, oarsub, \
    wait_oar_job_start, get_oar_job_nodes, get_oar_job_kavlan, \
    Deployment, deploy, get_g5k_sites, oardel, \
    default_frontend_connection_params
from execo_engine import Engine, sweep, ParamSweeper, logger, \
    slugify
from pprint import pformat


class kadeploy_dev_test(Engine):
    """A execo engine to perform test of various configuration of kadeploy and
    kadeploy-dev.
    """

    def create_paramsweeper(self):
        """Test all the sites, with or without a KaVLAN and for several env."""
        params = {
            "version": ['kadeploy3-dev', 'kadeploy3'],
            "kavlan": [True, False],
            "site": get_g5k_sites(),
            "n_nodes": [1, 4, 10],
            "env": ['wheezy-x64-base', 'wheezy-x64-prod', 'wheezy-x64-xen']
            }
        logger.info('Defining parameters: %s', pformat(params))

        combs = sweep(params)
        return ParamSweeper(self.result_dir + "/sweeper", combs)

    def run(self):
        sweeper = self.create_paramsweeper()

        while True:
            comb = sweeper.get_next()
            if not comb:
                break
            comb_file = self.result_dir + '/' + slugify(comb) + '/trace'
            g5k_configuration['kadeploy3'] = comb['version']
            logger.info('Treating combination %s', pformat(comb))
            get_version = SshProcess(comb['version'] + ' -v',
                                     comb['site'],
                                     connection_params=default_frontend_connection_params).run()
            logger.info(get_version.stdout)

            resources = ""
            if comb['kavlan']:
                resources += "{type='kavlan'}/vlan=1+"
            resources += "nodes=" + str(comb['n_nodes'])
            sub = OarSubmission(resources=resources,
                                job_type='deploy',
                                walltime="0:30:00",
                                name='Kadeploy_Tests')
            logger.info('Performing submission of %s on site %s',
                        resources, comb['site'])
            jobs = oarsub([(sub, comb['site'])])

            if jobs[0][0]:
                try:
                    logger.info('Waiting for job to start')
                    wait_oar_job_start(jobs[0][0], jobs[0][1])
                    hosts = get_oar_job_nodes(jobs[0][0], jobs[0][1])
                    logger.info('Deployment of %s',
                                ' '.join([host.address for host in hosts]))
                    kavlan = get_oar_job_kavlan(jobs[0][0], jobs[0][1])
                    if kavlan:
                        logger.info('In kavlan %s', kavlan)
                    deployment = Deployment(hosts, env_name=comb['env'],
                                            vlan=kavlan)
                    deployed, undeployed = deploy(deployment,
                                                  stdout_handlers=[comb_file],
                                                  stdout_handlers=[comb_file])

                finally:
                    logger.info('Destroying job %s on %s', str(jobs[0][0]),
                                jobs[0][1])
                    oardel([(jobs[0][0], jobs[0][1])])

            if len(undeployed) == 0:
                logger.info('%s is OK', slugify(comb))
            elif len(deployed) == 0:
                logger.error('%s is KO', slugify(comb))
            else:
                logger.warning('%s encountered problems with some hosts',
                               slugify(comb))

            sweeper.done(comb)


if __name__ == "__main__":
    e = kadeploy_dev_test()
    e.start()
