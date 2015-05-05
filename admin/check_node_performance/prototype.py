#!/usr/bin/env python

from execo import *
from execo_g5k import *

job_name = 'sysbench'
walltime = '2:00:00'
cluster = 'granduc'
env_name = 'wheezy-x64-base'


default_connection_params['user'] = 'root'

job = get_job_by_name(job_name)
if not job[0]:
    logger.info('No running job found, performing a new reservation')
    resources_wanted = {cluster: 0}
    start, end, resources = find_first_slot(get_slots(resources_wanted.keys(),
                                                      walltime=walltime),
                                            resources_wanted)
    jobs_specs = [(OarSubmission(resources="{cluster='%s'}/nodes=%s"
                                 % (cluster, resources[cluster]),
                                 job_type='deploy',
                                 walltime=walltime,
                                 reservation_date=start,
                                 name=job_name), u'lyon')]
    job = oarsub(jobs_specs)[0]
hosts = get_oar_job_nodes(*job)
logger.info('Deploying hosts')
deployed_hosts, _ = deploy(Deployment(hosts=hosts, env_name=env_name),
                           check_enough_func=lambda x, y: len(x) > len(y))
hosts = list(deployed_hosts)
cmd = 'apt-get update && apt-get install -f sysbench'
install_sysbench = TaktukRemote(cmd, hosts).start()
for p in install_sysbench.processes:
    print p.host.address
    print p.stdout
install_sysbench.wait()